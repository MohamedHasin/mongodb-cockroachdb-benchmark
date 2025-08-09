# --- Reproducible seeding (common block) ---
import os, random
import numpy as np
from faker import Faker

SEED = int(os.environ.get("SEED", "42"))
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
fake.seed_instance(SEED)
print(f"SEED={SEED}")
# ------------------------------------------

import time, json
from datetime import datetime, timedelta
import pymongo, psycopg2

# Connections
mongo = pymongo.MongoClient("mongodb://admin:password123@localhost:27017/")
mdb = mongo["social_media"]

conn = psycopg2.connect(host="localhost", port=26257, user="root", database="social_media")
cur = conn.cursor()

results = {"mongodb": {}, "cockroachdb": {}}

# --- Test 1: Insert 1000 users ---
# Seed-stable values prevent UNIQUE collisions on reruns
users = [
    {
        "username": f"user_{SEED}_{i}",
        "email": f"user_{SEED}_{i}@example.com",
    }
    for i in range(1000)
]

# MongoDB: bulk insert
t = time.time()
mdb_result = mdb.users.insert_many(users)
results["mongodb"]["insert_1000_users"] = time.time() - t
mdb_user_ids = mdb_result.inserted_ids  # for post FK-like reference

# CockroachDB: per-row insert and CAPTURE IDs
cr_user_ids = []
t = time.time()
for u in users:
    cur.execute(
        "INSERT INTO users (username, email) VALUES (%s, %s) RETURNING id",
        (u["username"], u["email"]),
    )
    cr_user_ids.append(cur.fetchone()[0])
conn.commit()
results["cockroachdb"]["insert_1000_users"] = time.time() - t

# --- Test 2: Single-user point lookup (100 reps, avg in ms) ---
target_username = users[0]["username"]

t = time.time()
for _ in range(100):
    mdb.users.find_one({"username": target_username})
results["mongodb"]["single_query"] = (time.time() - t) / 100.0 * 1000.0  # ms/op

t = time.time()
for _ in range(100):
    cur.execute("SELECT id, username FROM users WHERE username=%s", (target_username,))
    _ = cur.fetchone()
results["cockroachdb"]["single_query"] = (time.time() - t) / 100.0 * 1000.0  # ms/op

# --- Test 3: Insert 5000 posts (per-row) ---
def rand_dt_within_days(days: int = 14) -> datetime:
    # Recent timestamp within the last `days`
    return datetime.utcnow() - timedelta(seconds=random.randint(0, days * 24 * 3600))

# MongoDB posts (reference inserted user _ids)
mongo_posts = [
    {
        "user_id": random.choice(mdb_user_ids),
        "content": fake.sentence(nb_words=10),
        "created_at": rand_dt_within_days(),
    }
    for _ in range(5000)
]
t = time.time()
mdb.posts.insert_many(mongo_posts)
results["mongodb"]["insert_5000_posts"] = time.time() - t

# CockroachDB posts (reference captured SQL user ids)
t = time.time()
for _ in range(5000):
    cur.execute(
        "INSERT INTO posts (user_id, content, created_at) VALUES (%s, %s, %s)",
        (
            random.choice(cr_user_ids),
            fake.sentence(nb_words=10),
            rand_dt_within_days(),
        ),
    )
conn.commit()
results["cockroachdb"]["insert_5000_posts"] = time.time() - t

# --- Save results ---
os.makedirs("results", exist_ok=True)
results["seed"] = SEED
with open("results/performance_results.json", "w") as f:
    json.dump(results, f, indent=2)

# Cleanup
cur.close()
conn.close()
mongo.close()

print("✅ Saved results to results/performance_results.json")

