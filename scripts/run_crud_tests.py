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
import pymongo, psycopg2
from psycopg2.extras import execute_values

# Connections
mongo = pymongo.MongoClient("mongodb://admin:password123@localhost:27017/")
mdb = mongo["social_media"]

conn = psycopg2.connect(host="localhost", port=26257, user="root", database="social_media")
cur = conn.cursor()

results = {"mongodb": {}, "cockroachdb": {}}

# ---------- Fresh fixtures (separate from main tables/collections) ----------
cur.execute("DROP TABLE IF EXISTS posts2")
cur.execute("DROP TABLE IF EXISTS users2")
cur.execute("""
CREATE TABLE users2 (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) UNIQUE NOT NULL,
  email    VARCHAR(100) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")
cur.execute("""
CREATE TABLE posts2 (
  id SERIAL PRIMARY KEY,
  user_id INT REFERENCES users2(id),
  content TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_posts2_user_id ON posts2(user_id)")
conn.commit()

mdb.users2.drop()
mdb.posts2.drop()
mdb.users2.create_index("username", unique=True)
mdb.posts2.create_index("user_id")

# ----------------------- Workload sizes -----------------------
N_USERS = 2000
N_POSTS = 3000
UPDATE_USERS = 1000
UPDATE_POSTS = 1000
DELETE_USERS = 500

rng = random.Random(SEED)

# ----------------------- Seed USERS -----------------------
# Mongo
users = [{"username": f"u2_{SEED}_{i}", "email": f"u2_{SEED}_{i}@ex.com"} for i in range(N_USERS)]
t0 = perf_counter()
mdb_result = mdb.users2.insert_many(users)
results["mongodb"]["seed_users"] = perf_counter() - t0
mongo_user_ids = mdb_result.inserted_ids

# Cockroach (batched insert, capture ALL IDs across pages)
tuples = [(u["username"], u["email"]) for u in users]
t0 = perf_counter()
rows = execute_values(
    cur,
    "INSERT INTO users2 (username, email) VALUES %s RETURNING id",
    tuples,
    page_size=1000,
    fetch=True,  # <-- ensure we get all ids even with multiple pages
)
cr_user_ids = [row[0] for row in rows]
conn.commit()
results["cockroachdb"]["seed_users"] = perf_counter() - t0

# ----------------------- Seed POSTS -----------------------
# Posts tied to users; timestamps implicit (DEFAULT)
mongo_posts = [
    {"user_id": mongo_user_ids[rng.randrange(N_USERS)], "content": fake.text(max_nb_chars=160)}
    for _ in range(N_POSTS)
]
t0 = perf_counter()
mdb.posts2.insert_many(mongo_posts)
results["mongodb"]["seed_posts"] = perf_counter() - t0

cr_post_tuples = [(cr_user_ids[rng.randrange(N_USERS)], fake.text(max_nb_chars=160)) for _ in range(N_POSTS)]
t0 = perf_counter()
execute_values(cur, "INSERT INTO posts2 (user_id, content) VALUES %s", cr_post_tuples, page_size=1000)
conn.commit()
results["cockroachdb"]["seed_posts"] = perf_counter() - t0

# ----------------------- UPDATE 1000 USERS -----------------------
# Update emails to unique, seed-stable values to avoid UNIQUE conflicts.
mongo_update_user_pairs = list(zip(mongo_user_ids[:UPDATE_USERS], [f"u2upd_{SEED}_{i}@ex.com" for i in range(UPDATE_USERS)]))
t0 = perf_counter()
mdb.users2.bulk_write(
    [pymongo.UpdateOne({"_id": _id}, {"$set": {"email": new_mail}}) for _id, new_mail in mongo_update_user_pairs],
    ordered=False,
)
users_update_mongo_total = perf_counter() - t0
results["mongodb"]["update_1000_users_total_s"] = users_update_mongo_total
results["mongodb"]["update_1000_users_avg_ms"] = users_update_mongo_total / UPDATE_USERS * 1000.0

# Cockroach batched update via VALUES join
cr_update_user_pairs = list(zip(cr_user_ids[:UPDATE_USERS], [f"u2upd_{SEED}_{i}@ex.com" for i in range(UPDATE_USERS)]))
t0 = perf_counter()
execute_values(
    cur,
    "UPDATE users2 AS u SET email = v.email FROM (VALUES %s) AS v(id, email) WHERE u.id = v.id",
    cr_update_user_pairs,
    page_size=1000,
)
conn.commit()
users_update_cr_total = perf_counter() - t0
results["cockroachdb"]["update_1000_users_total_s"] = users_update_cr_total
results["cockroachdb"]["update_1000_users_avg_ms"] = users_update_cr_total / UPDATE_USERS * 1000.0

# ----------------------- UPDATE 1000 POSTS -----------------------
# Update content with a short tag; choose first UPDATE_POSTS posts by query.
mongo_post_ids = [d["_id"] for d in mdb.posts2.find({}, {"_id": 1}).limit(UPDATE_POSTS)]
t0 = perf_counter()
mdb.posts2.bulk_write(
    [pymongo.UpdateOne({"_id": _id}, {"$set": {"content": f"{fake.word()} upd_{SEED}"}}) for _id in mongo_post_ids],
    ordered=False,
)
posts_update_mongo_total = perf_counter() - t0
results["mongodb"]["update_1000_posts_total_s"] = posts_update_mongo_total
results["mongodb"]["update_1000_posts_avg_ms"] = posts_update_mongo_total / UPDATE_POSTS * 1000.0

# Cockroach: read UPDATE_POSTS ids then batch update
cur.execute("SELECT id FROM posts2 LIMIT %s", (UPDATE_POSTS,))
cr_post_ids = [r[0] for r in cur.fetchall()]
cr_post_pairs = [(pid, f"{fake.word()} upd_{SEED}") for pid in cr_post_ids]
t0 = perf_counter()
execute_values(
    cur,
    "UPDATE posts2 AS p SET content = v.content FROM (VALUES %s) AS v(id, content) WHERE p.id = v.id",
    cr_post_pairs,
    page_size=1000,
)
conn.commit()
posts_update_cr_total = perf_counter() - t0
results["cockroachdb"]["update_1000_posts_total_s"] = posts_update_cr_total
results["cockroachdb"]["update_1000_posts_avg_ms"] = posts_update_cr_total / UPDATE_POSTS * 1000.0

# ----------------------- DELETE 500 USERS (FK-safe) -----------------------
# Select 500 users; delete their posts first, then users (measure total time per engine).
mongo_del_oids = [mongo_user_ids[i] for i in rng.sample(range(N_USERS), DELETE_USERS)]
t0 = perf_counter()
mdb.posts2.delete_many({"user_id": {"$in": mongo_del_oids}})
mdb.users2.delete_many({"_id": {"$in": mongo_del_oids}})
mongo_delete_total = perf_counter() - t0
results["mongodb"]["delete_500_users_total_s"] = mongo_delete_total

cr_del_ids = [cr_user_ids[i] for i in rng.sample(range(N_USERS), DELETE_USERS)]
t0 = perf_counter()
# Delete posts via VALUES list
execute_values(cur, "DELETE FROM posts2 WHERE user_id IN (SELECT id FROM (VALUES %s) AS t(id))", [(i,) for i in cr_del_ids], page_size=1000)
# Then delete users
execute_values(cur, "DELETE FROM users2 WHERE id IN (SELECT id FROM (VALUES %s) AS t(id))", [(i,) for i in cr_del_ids], page_size=1000)
conn.commit()
cr_delete_total = perf_counter() - t0
results["cockroachdb"]["delete_500_users_total_s"] = cr_delete_total

# ----------------------- Save & Cleanup -----------------------
os.makedirs("results", exist_ok=True)
results["seed"] = SEED
with open("results/crud_results.json", "w") as f:
    json.dump(results, f, indent=2)

cur.close()
conn.close()
mongo.close()

print("✅ Saved results to results/crud_results.json")

