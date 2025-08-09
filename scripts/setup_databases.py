import pymongo
import psycopg2

print("Connecting...")

# --- MongoDB ---
mongo = pymongo.MongoClient("mongodb://admin:password123@localhost:27017/")
mdb = mongo["social_media"]

# --- CockroachDB (connect to defaultdb, then CREATE/USE target DB) ---
conn = psycopg2.connect(host="localhost", port=26257, user="root", database="defaultdb")
conn.set_session(autocommit=True)
cur = conn.cursor()

# ---------- MongoDB reset ----------
print("Resetting MongoDB...")
for col in ("users", "posts", "comments", "follows", "likes"):
    mdb[col].drop()

# Indexes (mirror what queries need)
mdb.users.create_index("username", unique=True)
mdb.users.create_index("email", unique=True)
mdb.posts.create_index([("user_id", 1), ("created_at", -1)])   # timeline queries
mdb.comments.create_index("post_id")

# ---------- CockroachDB reset ----------
print("Resetting CockroachDB...")
cur.execute("DROP DATABASE IF EXISTS social_media CASCADE")
cur.execute("CREATE DATABASE social_media")
cur.execute("USE social_media")

# Tables
cur.execute("""
CREATE TABLE users (
    id        SERIAL PRIMARY KEY,
    username  VARCHAR(50)  UNIQUE NOT NULL,
    email     VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")

cur.execute("""
CREATE TABLE posts (
    id         SERIAL PRIMARY KEY,
    user_id    INT REFERENCES users(id),
    content    TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")

cur.execute("""
CREATE TABLE comments (
    id         SERIAL PRIMARY KEY,
    post_id    INT REFERENCES posts(id),
    user_id    INT REFERENCES users(id),
    content    TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)""")

# Read-path indexes to match your workload
cur.execute("CREATE INDEX idx_posts_user_created ON posts (user_id, created_at DESC)")
cur.execute("CREATE INDEX idx_comments_post_id   ON comments (post_id)")

print("Database setup complete!")

# Cleanup
cur.close()
conn.close()
mongo.close()

