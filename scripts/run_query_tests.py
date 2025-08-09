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

results = {"mongodb": {}, "cockroachdb": {}}
rng = random.Random(SEED)

# ---------- Fresh tables/collections ----------
cur.execute("DROP TABLE IF EXISTS posts_q")
cur.execute("DROP TABLE IF EXISTS users_q")
cur.execute("""
CREATE TABLE users_q (
  id SERIAL PRIMARY KEY,
  username STRING UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")
cur.execute("""
CREATE TABLE posts_q (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users_q(id),
  content STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")
cur.execute("CREATE INDEX IF NOT EXISTS posts_q_user_created_idx ON posts_q (user_id, created_at DESC)")
conn.commit()

mdb.users_q.drop()
mdb.posts_q.drop()
mdb.users_q.create_index("username", unique=True)
mdb.posts_q.create_index([("user_id", 1), ("created_at", -1)])

# ---------- Seed users in both DBs (capture ALL returned ids) ----------
N_USERS = 1000
N_POSTS = 10000
HOT_USER_IDX = 0          # ensure this user has plenty of posts for "latest-20"
HOT_USER_EXTRA = 400      # extra posts guaranteed for HOT_USER_IDX

users = [{"username": f"uq_{SEED}_{i}"} for i in range(N_USERS)]
mdb_users_res = mdb.users_q.insert_many(users)
mongo_user_ids = mdb_users_res.inserted_ids

rows = execute_values(
    cur,
    "INSERT INTO users_q (username) VALUES %s RETURNING id",
    [(u["username"],) for u in users],
    page_size=1000,
    fetch=True,
)
cr_user_ids = [r[0] for r in rows]
conn.commit()

# ---------- Seed posts (uniform + guaranteed for hot user) ----------
def rand_dt_within_days(days: int = 14) -> datetime:
    return datetime.utcnow() - timedelta(seconds=rng.randint(0, days * 24 * 3600))

# Build posts
posts = []
# Extra posts for hot user to guarantee >= 20
for _ in range(HOT_USER_EXTRA):
    posts.append({"user_idx": HOT_USER_IDX, "content": fake.sentence(nb_words=10), "created_at": rand_dt_within_days()})
# Remaining posts distributed across users
for _ in range(N_POSTS - HOT_USER_EXTRA):
    posts.append({"user_idx": rng.randrange(0, N_USERS), "content": fake.sentence(nb_words=10), "created_at": rand_dt_within_days()})

# Mongo insert
t0 = perf_counter()
mdb.posts_q.insert_many(
    [{"user_id": mongo_user_ids[p["user_idx"]], "content": p["content"], "created_at": p["created_at"]} for p in posts]
)
results["mongodb"]["seed_posts"] = perf_counter() - t0

# Cockroach insert (batched)
post_tuples = [(cr_user_ids[p["user_idx"]], p["content"], p["created_at"]) for p in posts]
t0 = perf_counter()
execute_values(cur, "INSERT INTO posts_q (user_id, content, created_at) VALUES %s", post_tuples, page_size=1000)
conn.commit()
results["cockroachdb"]["seed_posts"] = perf_counter() - t0

# ---------- Queries: latest-20 and last-7-days (avg in ms over reps) ----------
REPS = 200
target_mongo_uid = mongo_user_ids[HOT_USER_IDX]
target_cr_uid = cr_user_ids[HOT_USER_IDX]
since_7d = datetime.utcnow() - timedelta(days=7)

# Mongo: latest-20 by user
t0 = perf_counter()
for _ in range(REPS):
    list(
        mdb.posts_q.find({"user_id": target_mongo_uid})
                   .sort("created_at", -1)
                   .limit(20)
    )
results["mongodb"]["latest20_avg_ms"] = (perf_counter() - t0) / REPS * 1000.0

# Cockroach: latest-20 by user
t0 = perf_counter()
for _ in range(REPS):
    cur.execute(
        "SELECT id, user_id, content, created_at FROM posts_q WHERE user_id = %s ORDER BY created_at DESC LIMIT 20",
        (target_cr_uid,),
    )
    _ = cur.fetchall()
results["cockroachdb"]["latest20_avg_ms"] = (perf_counter() - t0) / REPS * 1000.0

# Mongo: range last 7 days by user
t0 = perf_counter()
for _ in range(REPS):
    list(
        mdb.posts_q.find({"user_id": target_mongo_uid, "created_at": {"$gte": since_7d}})
                   .sort("created_at", -1)
    )
results["mongodb"]["range7d_avg_ms"] = (perf_counter() - t0) / REPS * 1000.0

# Cockroach: range last 7 days by user
t0 = perf_counter()
for _ in range(REPS):
    cur.execute(
        "SELECT id, user_id, content, created_at FROM posts_q WHERE user_id = %s AND created_at >= %s ORDER BY created_at DESC",
        (target_cr_uid, since_7d),
    )
    _ = cur.fetchall()
results["cockroachdb"]["range7d_avg_ms"] = (perf_counter() - t0) / REPS * 1000.0

# ---------- Save & cleanup ----------
os.makedirs("results", exist_ok=True)
results["seed"] = SEED
with open("results/query_results.json", "w") as f:
    json.dump(results, f, indent=2)

cur.close()
conn.close()
mongo.close()

print("✅ Saved results to results/query_results.json")

