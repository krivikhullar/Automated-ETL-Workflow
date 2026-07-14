# Automated-ETL-Workflow
A Python/SQLite implementation of an ETL workflow that consolidates three independent source systems — a REST API, a flat file, and a relational database — into a unified customer warehouse, with reusable mapping functions, cross-source deduplication, data quality rules, and an automated reconciliation audit.


Note on origin: This pipeline was originally designed and prototyped in Informatica PowerCenter. Because PowerCenter mappings are proprietary binary/XML artifacts tied to a licensed desktop install, they can't be run or reviewed from a public repo. This repo is a runnable, inspectable re-implementation of the same pipeline architecture and logic in Python + SQLite, so the design can be evaluated without requiring PowerCenter access.



Overview

Real customer data rarely lives in one place. This project simulates the common enterprise problem of three systems of record (API, file, database) holding overlapping, inconsistently formatted records for the same customers, and builds a pipeline that:


Extracts from all three source types independently
Transforms records into a standardized schema via a single reusable mapping function
Applies data quality rules (valid email format, non-null name, valid date) and rejects records that fail
Deduplicates across sources on normalized identity (name + email), keeping the earliest signup record as authoritative
Loads the clean, deduplicated result into a unified SQLite warehouse table
Audits the result with a reconciliation check — not just a row-count comparison, but a ground-truth accuracy check against deliberately seeded duplicate pairs


Why the Audit Is the Interesting Part

Most ETL demos assert success by construction. This one doesn't:


Source data is seeded with realistic messiness — mixed casing, stray whitespace, malformed emails, missing names — so the quality rules have real defects to catch.
A known set of duplicate pairs is generated across sources, including a small percentage of duplicates that deliberately do not resolve cleanly (e.g. a typo'd domain: gmail.com → gmial.com), simulating real-world dedup misses.
The reconciliation audit measures two things independently:

Row-count reconciliation: do extracted → quality-passed → deduplicated counts add up to the final warehouse count?
Dedup match accuracy: against the seeded ground truth, did true duplicates correctly collapse to one record, and did corrupted "near-duplicates" correctly remain distinct rows (i.e., no false merges)?





This distinguishes "the pipeline ran without errors" from "the pipeline made the correct decisions."

Pipeline Stages

StageWhat HappensSeedGenerates three mock source systems with realistic overlap, formatting drift, and known duplicate pairs for later validationExtractPulls from flat CSV, mock REST API (JSON), and a source SQLite tableTransformStandardizes all records to a common schema; applies data quality rules; deduplicates on normalized (name, email)LoadWrites clean, deduplicated records to a unified dim_customer table in a SQLite warehouseAuditReconciles row counts end-to-end and scores dedup accuracy against ground-truth duplicate pairs

Tech Stack


Python (standard library only: sqlite3, csv, json, re, random, datetime, pathlib)
SQLite (source relational system + warehouse)


Project Structure

.
├── etl_pipeline.py
└── data/                          # generated at runtime by seed_sources()
    ├── source_flatfile.csv
    ├── source_api_mock.json
    ├── source_relational.db
    ├── known_duplicate_pairs.json
    └── warehouse.db

Setup

No third-party dependencies — standard library only.

bashpython3 --version   # 3.8+

Usage

bashpython etl_pipeline.py

On each run, the script:


Seeds fresh mock source data under data/ (deterministic via random.seed(42))
Runs the full extract → transform → load pipeline
Prints a reconciliation audit report, including:

Per-source and total extracted record counts
Records rejected by quality rules
Records removed as duplicates
Expected vs. actual warehouse row count (PASS/FAIL)
Dedup match accuracy against known ground-truth duplicate pairs





Sample Output

RECONCILIATION AUDIT
============================================================
Source record counts:
  flat_file           : 300
  rest_api            : 345
  relational_db       : 397

TOTAL EXTRACTED       : 1042
Rejected by quality rules: ~15
Removed as duplicates:     ~90
Expected unique records:   ~937
Warehouse record count:    937
Row-count reconciliation:  PASS

Ground-truth duplicate pairs tested: 90
Correctly resolved:                  ~85
Dedup match accuracy:                94%

(Exact figures vary slightly by run configuration; the seeded random state keeps results reproducible.)

Possible Extensions


Swap SQLite for a real REST API call (requests) and a networked relational DB (Postgres/SQL Server) to move from simulation to production sources
Add fuzzy matching (e.g. Levenshtein distance on email/name) to catch the corrupted near-duplicates that currently fail to merge
Parameterize quality rules and dedup keys via a config file instead of hardcoded logic
Add incremental/delta loading instead of full warehouse rebuild on each run
Log rejected records to a dead-letter table for manual review instead of dropping them silently
