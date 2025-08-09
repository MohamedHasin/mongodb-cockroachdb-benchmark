# MongoDB vs CockroachDB Benchmark

This repository contains the implementation for comparing MongoDB and CockroachDB performance on social media workloads.

## Project Structure
-  - Python test scripts
-  - JSON results and generated figures
-  - Test data files

## Running the Tests
```bash
export SEED=4242
python scripts/setup_databases.py
python scripts/run_tests.py
python scripts/run_tests_batched.py
```

## Authors
- Mohamed Hasin Hussain
- Law Rou Rou  
- Kiroshan Ram

SEG2102 Database Management Systems - Sunway University
