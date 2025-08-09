# --- Reproducible seeding (common block) ---
import os, random
import numpy as np
from faker import Faker

SEED = int(os.environ.get("SEED", "42"))
random.seed(SEED)
np.random.seed(SEED)
fake = Faker(); fake.seed_instance(SEED)
print(f"SEED={SEED}")
# ------------------------------------------

import json
from time import perf_counter
from datetime import datetime, timedelta
import pymongo, psycopg2
from psycopg2.extras import execute_values

# Connections
mongo = pymongo.MongoClient("mongodb://admin:password123@localhost:27017/")
mdb = mongo["social_media"]

conn = psycopg2.connect(host="localhost", port=26257, user="root", database="social_media")
cur = conn.cursor()

results = {"mongodb": {}, "cockroachdb_batched": {}}

# --- Helpers ---
def rand_dt_within_days(days: int = 14) -> datetime:
    return datetime.utcnow() - timedelta(seconds=random.randint(0, days * 24 * 3600))

# --- Users: 1000 (seed-stable to avoid UNIQUE collisions on reruns) ---
users = [{"username": f"user_{SEED}_{i}", "email": f"user_{SEED}_{i}@example.com"} for i in range(1000)]

# MongoDB users (bulk insert)
t = perf_counter()
mdb_result = mdb.users.insert_many(users)
results["mongodb"]["insert_1000_users"] = perf_counter() - t
mdb_user_ids = mdb_result.inserted_ids  # keep for Mongo post refs

# CockroachDB users (batched insert with RETURNING id)
tuples = [(u["username"], u["email"]) for u in users]
t = perf_counter()
execute_values(cur, "INSERT INTO users (username, email) VALUES %s RETURNING id", tuples, page_size=1000)
cr_user_ids = [row[0] for row in cur.fetchall()]
conn.commit()
results["cockroachdb_batched"]["insert_1000_users"] = perf_counter() - t

# --- Single-user point lookup (100 reps, avg in ms) ---
target_username = users[0]["username"]

t = perf_counter()
for _ in range(100):
    mdb.users.find_one({"username": target_username})
results["mongodb"]["single_query"] = (perf_counter() - t) / 100.0 * 1000.0  # ms/op

t = perf_counter()
for _ in range(100):
    cur.execute("SELECT id, username FROM users WHERE username=%s", (target_username,))
    cur.fetchone()
results["cockroachdb_batched"]["single_query"] = (perf_counter() - t) / 100.0 * 1000.0  # ms/op

# --- Posts: 5000 (with created_at; batched for Cockroach) ---
posts = [
    {"user_idx": random.randrange(0, len(users)), "content": fake.text(max_nb_chars=200), "created_at": rand_dt_within_days()}
    for _ in range(5000)
]

# MongoDB posts: reference actual Mongo _ids (not +1 hack)
t = perf_counter()
mdb.posts.insert_many(
    [
        {"user_id": mdb_user_ids[p["user_idx"]], "content": p["content"], "created_at": p["created_at"]}
        for p in posts
    ]
)
results["mongodb"]["insert_5000_posts"] = perf_counter() - t

# CockroachDB posts: batched insert
post_tuples = [(cr_user_ids[p["user_idx"]], p["content"], p["created_at"]) for p in posts]
t = perf_counter()
execute_values(cur, "INSERT INTO posts (user_id, content, created_at) VALUES %s", post_tuples, page_size=1000)
conn.commit()
results["cockroachdb_batched"]["insert_5000_posts"] = perf_counter() - t

# --- Save results ---
os.makedirs("results", exist_ok=True)
results["seed"] = SEED
with open("results/performance_results_batched.json", "w") as f:
    json.dump(results, f, indent=2)

# Cleanup
cur.close()
conn.close()
mongo.close()

print("✅ Saved results to results/performance_results_batched.json")
