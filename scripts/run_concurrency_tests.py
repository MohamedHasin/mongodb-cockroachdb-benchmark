# --- Reproducible seeding (common block) ---
import os, random
import numpy as np
from faker import Faker  # not used here, but kept for consistency

SEED = int(os.environ.get("SEED", "42"))
random.seed(SEED)
np.random.seed(SEED)
fake = Faker(); fake.seed_instance(SEED)
print(f"SEED={SEED}")
# ------------------------------------------

import json, statistics
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo, psycopg2

# --- Config ---
MONGO_URI = "mongodb://admin:password123@localhost:27017/"
CR_HOST, CR_PORT, CR_USER, CR_DB = "localhost", 26257, "root", "social_media"
REPS_PER_THREAD = 100   # how many point-lookups each thread performs

# --- Load targets (usernames) once, from BOTH DBs, then intersect for fairness ---
mongo = pymongo.MongoClient(MONGO_URI)
mdb = mongo["social_media"]
mongo_usernames = [d["username"] for d in mdb.users.find({}, {"username": 1, "_id": 0})]

cr_conn = psycopg2.connect(host=CR_HOST, port=CR_PORT, user=CR_USER, database=CR_DB)
with cr_conn.cursor() as c:
    c.execute("SELECT username FROM users")
    cr_usernames = [r[0] for r in c.fetchall()]

# Intersect so both engines query the SAME keyspace
usernames = sorted(set(mongo_usernames) & set(cr_usernames))
if not usernames:
    raise SystemExit("No common usernames between MongoDB and CockroachDB. Run setup/tests first.")

# Keep a fixed cap for comparability and to bound runtime
if len(usernames) > 1000:
    usernames = usernames[:1000]

def p95(values):
    # statistics.quantiles with n=20 returns 5%,10%,...,95% cut points -> index 18 is ~95th
    if len(values) >= 20:
        return statistics.quantiles(values, n=20)[18]
    # Fallback for very small samples
    vs = sorted(values)
    return vs[int(0.95 * (len(vs) - 1))]

def run_mongo_batch(n_threads):
    latencies = []
    def worker(thread_idx: int):
        client = pymongo.MongoClient(MONGO_URI)  # isolated connection per thread
        db = client["social_media"]
        rng = random.Random(SEED + thread_idx)   # deterministic per-thread
        local_lat = []
        for _ in range(REPS_PER_THREAD):
            uname = rng.choice(usernames)
            t0 = perf_counter()
            db.users.find_one({"username": uname})
            local_lat.append((perf_counter() - t0) * 1000.0)  # ms
        client.close()
        return local_lat

    t0 = perf_counter()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(worker, i) for i in range(n_threads)]
        for f in as_completed(futures):
            latencies.extend(f.result())
    total_s = perf_counter() - t0

    qps = n_threads * REPS_PER_THREAD / total_s
    return {
        "avg_ms": sum(latencies) / len(latencies),
        "p95_ms": p95(latencies),
        "throughput_qps": qps,
        "n_threads": n_threads,
        "n_ops": len(latencies),
    }

def run_crdb_batch(n_threads):
    latencies = []
    def worker(thread_idx: int):
        conn = psycopg2.connect(host=CR_HOST, port=CR_PORT, user=CR_USER, database=CR_DB)
        cur = conn.cursor()
        rng = random.Random(SEED + 10_000 + thread_idx)  # different stream from Mongo
        local_lat = []
        for _ in range(REPS_PER_THREAD):
            uname = rng.choice(usernames)
            t0 = perf_counter()
            cur.execute("SELECT id, username FROM users WHERE username = %s", (uname,))
            _ = cur.fetchone()
            local_lat.append((perf_counter() - t0) * 1000.0)  # ms
        cur.close()
        conn.close()
        return local_lat

    t0 = perf_counter()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        futures = [ex.submit(worker, i) for i in range(n_threads)]
        for f in as_completed(futures):
            latencies.extend(f.result())
    total_s = perf_counter() - t0

    qps = n_threads * REPS_PER_THREAD / total_s
    return {
        "avg_ms": sum(latencies) / len(latencies),
        "p95_ms": p95(latencies),
        "throughput_qps": qps,
        "n_threads": n_threads,
        "n_ops": len(latencies),
    }

# --- Execute batches for 10 and 50 threads ---
results = {"mongodb": {}, "cockroachdb": {}}
for n in (10, 50):
    results["mongodb"][f"read_threads_{n}"] = run_mongo_batch(n)
    results["cockroachdb"][f"read_threads_{n}"] = run_crdb_batch(n)

# --- Save & cleanup ---
os.makedirs("results", exist_ok=True)
results["seed"] = SEED
with open("results/concurrency_results.json", "w") as f:
    json.dump(results, f, indent=2)

cr_conn.close()
mongo.close()

print("✅ Saved results to results/concurrency_results.json")

