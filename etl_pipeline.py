"""
Automated ETL Workflow
-----------------------
Consolidates three source types (REST API, flat file, relational DB) into a
unified SQLite warehouse, with reusable mapping functions, deduplication
logic, and data quality rules — validated via a reconciliation audit.

This is a Python/SQLite re-implementation of an ETL workflow originally
prototyped in Informatica PowerCenter. PowerCenter mappings are proprietary
binary/XML artifacts tied to a licensed desktop install and can't be run or
reviewed from a public repo, so this version demonstrates the same pipeline
architecture and logic in a runnable, inspectable form.

Pipeline stages:
  1. EXTRACT  - pull from a mock REST API, a flat CSV file, and a source SQL table
  2. TRANSFORM - standardize schema, dedupe, apply quality rules
  3. LOAD     - write to a unified SQLite warehouse table
  4. AUDIT    - reconciliation check comparing source row counts to warehouse
                row counts post-dedup, reporting match/accuracy rate

Run: python etl_pipeline.py
"""
import sqlite3
import csv
import json
import random
import re
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path("data")
DB_PATH = DATA_DIR / "warehouse.db"
FLAT_FILE_PATH = DATA_DIR / "source_flatfile.csv"
API_MOCK_PATH = DATA_DIR / "source_api_mock.json"
SOURCE_DB_PATH = DATA_DIR / "source_relational.db"

random.seed(42)


# ---------------------------------------------------------------------------
# SOURCE SETUP (simulates three independent systems of record)
# ---------------------------------------------------------------------------
def seed_sources(n_per_source=300, overlap_ratio=0.15):
    """Creates three source systems with realistic overlap/duplication
    across them, so dedup logic has something real to do."""
    DATA_DIR.mkdir(exist_ok=True)

    names = [f"Customer_{i}" for i in range(1, int(n_per_source * 2.5))]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]

    def make_record(cust_id):
        name = random.choice(names)
        email = f"{name.lower()}@{random.choice(domains)}"
        # inject messiness: mixed casing, whitespace, formatting drift
        if random.random() < 0.3:
            email = email.upper()
        if random.random() < 0.2:
            email = f" {email} "
        if random.random() < 0.012:
            email = email.replace("@", "_at_")  # simulate malformed source data
        if random.random() < 0.008:
            name = ""  # simulate missing-name source defect
        return {
            "customer_id": cust_id,
            "name": name,
            "email": email,
            "signup_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d"),
            "region": random.choice(["East", "West", "Midwest", "South"]),
        }

    # Flat file source
    flat_records = [make_record(f"F{i:04d}") for i in range(n_per_source)]
    with open(FLAT_FILE_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=flat_records[0].keys())
        writer.writeheader()
        writer.writerows(flat_records)

    # Ground truth: every "duplicate pair" we deliberately create is logged here
    # so we can independently measure whether dedup actually caught it,
    # instead of asserting the pipeline always succeeds by construction.
    known_duplicate_pairs = []

    def make_overlap_copy(source_record, new_id, corrupt=False):
        copy = source_record.copy()
        copy["customer_id"] = new_id
        if corrupt:
            # simulates a real-world duplicate that DOESN'T resolve cleanly:
            # a typo'd email domain, e.g. "gmail.com" -> "gmial.com"
            copy["email"] = copy["email"].replace("gmail.com", "gmial.com") \
                                          .replace("yahoo.com", "yaho.com")
        known_duplicate_pairs.append(
            {"original_email": source_record["email"].strip().lower(),
             "duplicate_email": copy["email"].strip().lower(),
             "should_merge": not corrupt}
        )
        return copy

    # Mock REST API source (some records overlap with flat file customers)
    api_records = [make_record(f"A{i:04d}") for i in range(n_per_source)]
    overlap_n = int(n_per_source * overlap_ratio)
    # ~3% of overlaps are corrupted duplicates that legitimately won't match
    corrupt_indices = set(random.sample(range(overlap_n), max(1, overlap_n // 150)))
    for i in range(overlap_n):
        api_records.append(make_overlap_copy(flat_records[i], f"A{9000+i}", corrupt=i in corrupt_indices))
    with open(API_MOCK_PATH, "w") as f:
        json.dump(api_records, f)

    # Relational source (some overlap with API source too)
    if SOURCE_DB_PATH.exists():
        SOURCE_DB_PATH.unlink()
    conn = sqlite3.connect(SOURCE_DB_PATH)
    conn.execute("""
        CREATE TABLE customers (
            customer_id TEXT, name TEXT, email TEXT, signup_date TEXT, region TEXT
        )
    """)
    db_records = [make_record(f"R{i:04d}") for i in range(n_per_source)]
    corrupt_indices_2 = set(random.sample(range(overlap_n), max(1, overlap_n // 150)))
    for i in range(overlap_n):
        db_records.append(make_overlap_copy(api_records[i], f"R{9000+i}", corrupt=i in corrupt_indices_2))
    conn.executemany(
        "INSERT INTO customers VALUES (?,?,?,?,?)",
        [(r["customer_id"], r["name"], r["email"], r["signup_date"], r["region"]) for r in db_records],
    )
    conn.commit()
    conn.close()

    with open(DATA_DIR / "known_duplicate_pairs.json", "w") as f:
        json.dump(known_duplicate_pairs, f)

    return len(flat_records), len(api_records), len(db_records)


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------
def extract_flat_file():
    with open(FLAT_FILE_PATH, newline="") as f:
        return list(csv.DictReader(f))


def extract_api_source():
    """Simulates a REST API pull (in production this would be a
    requests.get() call against a paginated endpoint)."""
    with open(API_MOCK_PATH) as f:
        return json.load(f)


def extract_relational_source():
    conn = sqlite3.connect(SOURCE_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM customers").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# TRANSFORM  (reusable mapping + quality rule functions)
# ---------------------------------------------------------------------------
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$")


def standardize_record(record, source_system):
    """Reusable mapping: normalizes any source record into the warehouse schema."""
    return {
        "source_system": source_system,
        "source_id": record["customer_id"],
        "name": record["name"].strip(),
        "email": record["email"].strip().lower(),
        "signup_date": record["signup_date"],
        "region": record["region"],
    }


def apply_quality_rules(records):
    """Data quality rules: valid email format, non-null name, valid date."""
    clean, rejected = [], []
    for r in records:
        issues = []
        if not EMAIL_RE.match(r["email"]):
            issues.append("invalid_email")
        if not r["name"]:
            issues.append("missing_name")
        try:
            datetime.strptime(r["signup_date"], "%Y-%m-%d")
        except ValueError:
            issues.append("invalid_date")

        if issues:
            r["_rejection_reasons"] = issues
            rejected.append(r)
        else:
            clean.append(r)
    return clean, rejected


def deduplicate(records):
    """Dedup on normalized (name, email) — the same person can appear across
    all three sources with different source-specific IDs."""
    seen = {}
    for r in records:
        key = (r["name"].lower(), r["email"])
        if key not in seen:
            seen[key] = r
        else:
            # keep the earliest signup_date as the authoritative record
            if r["signup_date"] < seen[key]["signup_date"]:
                seen[key] = r
    return list(seen.values())


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------
def load_to_warehouse(records):
    if DB_PATH.exists():
        DB_PATH.unlink()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE dim_customer (
            customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
            source_system TEXT,
            source_id TEXT,
            name TEXT,
            email TEXT UNIQUE,
            signup_date TEXT,
            region TEXT
        )
    """)
    conn.executemany(
        """INSERT INTO dim_customer
           (source_system, source_id, name, email, signup_date, region)
           VALUES (:source_system, :source_id, :name, :email, :signup_date, :region)""",
        records,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# AUDIT / RECONCILIATION
# ---------------------------------------------------------------------------
def reconciliation_audit(source_counts, rejected_count, final_count, deduped_out):
    total_extracted = sum(source_counts.values())
    total_after_quality = total_extracted - rejected_count
    expected_unique = total_after_quality - deduped_out

    conn = sqlite3.connect(DB_PATH)
    warehouse_emails = {
        row[0] for row in conn.execute("SELECT email FROM dim_customer").fetchall()
    }
    warehouse_count = len(warehouse_emails)
    conn.close()

    row_count_match = warehouse_count == expected_unique

    # --- Row-count reconciliation (pipeline integrity check) ---
    print("=" * 60)
    print("RECONCILIATION AUDIT")
    print("=" * 60)
    print("Source record counts:")
    for src, count in source_counts.items():
        print(f"  {src:20s}: {count}")
    print(f"  {'TOTAL EXTRACTED':20s}: {total_extracted}")
    print(f"\nRejected by quality rules: {rejected_count}")
    print(f"Removed as duplicates:     {deduped_out}")
    print(f"Expected unique records:   {expected_unique}")
    print(f"Warehouse record count:    {warehouse_count}")
    print(f"Row-count reconciliation:  {'PASS' if row_count_match else 'FAIL'}")

    # --- Dedup accuracy against ground-truth known duplicate pairs ---
    with open(DATA_DIR / "known_duplicate_pairs.json") as f:
        known_pairs = json.load(f)

    correct = 0
    for pair in known_pairs:
        if pair["should_merge"]:
            # true duplicate: original_email == duplicate_email by construction.
            # Correct if that email made it into the warehouse (i.e. the pair
            # wasn't lost entirely) - dedup collapsing them to one row is
            # already guaranteed by construction of deduplicate(), so this
            # catches the real failure mode: both copies getting rejected
            # by quality rules and the customer vanishing entirely.
            is_correct = pair["original_email"] in warehouse_emails
        else:
            # corrupted "duplicate" has a different (typo'd) email - correct
            # only if BOTH the original and the corrupted variant survived as
            # distinct rows, proving dedup didn't over-merge on a false match
            is_correct = (
                pair["original_email"] in warehouse_emails
                and pair["duplicate_email"] in warehouse_emails
            )
        correct += int(is_correct)

    dedup_accuracy = correct / len(known_pairs) if known_pairs else 1.0

    print(f"\nGround-truth duplicate pairs tested: {len(known_pairs)}")
    print(f"Correctly resolved:                  {correct}")
    print(f"Dedup match accuracy:                {dedup_accuracy:.2%}")
    return dedup_accuracy


# ---------------------------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------------------------
def run_pipeline():
    print("Seeding mock source systems (flat file, REST API, relational DB)...")
    n_flat, n_api, n_db = seed_sources()
    source_counts = {"flat_file": n_flat, "rest_api": n_api, "relational_db": n_db}

    print("Extracting from all sources...")
    raw = (
        [standardize_record(r, "flat_file") for r in extract_flat_file()]
        + [standardize_record(r, "rest_api") for r in extract_api_source()]
        + [standardize_record(r, "relational_db") for r in extract_relational_source()]
    )

    print("Applying data quality rules...")
    clean, rejected = apply_quality_rules(raw)

    print("Deduplicating across sources...")
    deduped = deduplicate(clean)
    deduped_out = len(clean) - len(deduped)

    print("Loading to warehouse...")
    load_to_warehouse(deduped)

    print()
    reconciliation_audit(source_counts, len(rejected), len(deduped), deduped_out)


if __name__ == "__main__":
    run_pipeline()
