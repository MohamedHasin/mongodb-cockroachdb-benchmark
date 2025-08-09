# MongoDB vs CockroachDB Benchmark

Reproducible, apples-to-apples comparison of MongoDB (NoSQL) and CockroachDB (Distributed SQL) on a social-media-style workload (users/posts). Measures ingest, CRUD, indexed reads, and small read concurrency with matched indexes and a fixed random seed.

Requirements
Docker Desktop

Python 3.11

pip install -r requirements.txt

Quick Start
bash
Copy
Edit
# 0) (recommended) set a fixed seed for reproducibility
export SEED=4242

# 1) Spin up databases (single-node)
docker start mongodb cockroachdb 2>/dev/null || true
docker inspect mongodb >/dev/null 2>&1 || docker run -d --name mongodb -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=password123 mongo:7
docker inspect cockroachdb >/dev/null 2>&1 || docker run -d --name cockroachdb \
  -p 26257:26257 -p 8080:8080 cockroachdb/cockroach:latest start-single-node --insecure

# 2) Prepare schemas & seed data
python scripts/setup_databases.py

# 3) Run benchmarks
python scripts/run_tests.py              # baseline (per-row writes)
python scripts/run_tests_batched.py      # fair writes (batched for CRDB)
python scripts/run_crud_tests.py
python scripts/run_query_tests.py
python scripts/run_concurrency_tests.py

# 4) Generate summaries & charts
python scripts/generate_graphs.py
python scripts/generate_combined_charts.py
python scripts/generate_concurrency_chart.py
python scripts/generate_overall_summary.py
Outputs
JSON results in results/*.json

Aggregates in results/overall_summary.csv and results/overall_summary.md

Charts saved under results/

What’s Compared (Essentials)
Ingest: 1,000 users; 5,000 posts

Reads: point lookup, latest-20 timeline, last-7-days range

CRUD: update users/posts; delete 500 users (FK-safe)

Concurrency: 10 & 50 threads (avg, p95, QPS)

Fairness: matched logical schema/indexes; batched vs per-row writes; seeded runs

Repo Structure
scripts/ – setup, benchmarks, and figure generators

results/ – raw results + summaries + charts

requirements.txt, README.md

Limitations
Single-node containers (no multi-region/failover); desktop-class hardware; read-only concurrency probe.

Authors
Mohamed Hasin Hussain · Law Rou Rou · Kiroshan Ram
SEG2102 Database Management Systems – Sunway University









