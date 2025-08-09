import json, os
import matplotlib.pyplot as plt

# Helper to load JSON safely
def load_json(path):
    with open(path) as f:
        return json.load(f)

naive = load_json("results/performance_results.json")
batched = load_json("results/performance_results_batched.json")
crud = load_json("results/crud_results.json")
qres = load_json("results/query_results.json")

# 1) Insert 1000 Users
plt.figure()
labels = ["Mongo", "CRDB-Naïve", "CRDB-Batched"]
vals = [
    naive["mongodb"]["insert_1000_users"],
    naive["cockroachdb"]["insert_1000_users"],
    batched["cockroachdb_batched"]["insert_1000_users"],
]
plt.bar(labels, vals)
plt.ylabel("Time (s)")
plt.title("Insert 1000 Users")
plt.savefig("results/combined_insert_users.png", dpi=150)

# 2) Insert 5000 Posts
plt.figure()
vals = [
    naive["mongodb"]["insert_5000_posts"],
    naive["cockroachdb"]["insert_5000_posts"],
    batched["cockroachdb_batched"]["insert_5000_posts"],
]
plt.bar(labels, vals)
plt.ylabel("Time (s)")
plt.title("Insert 5000 Posts")
plt.savefig("results/combined_insert_posts.png", dpi=150)

# 3) Updates (avg ms/op)
plt.figure()
labels2 = ["Usernames(1k)", "Posts(1k)"]
mongo_vals = [
    crud["mongodb"]["update_1000_users_avg_ms"],
    crud["mongodb"]["update_1000_posts_avg_ms"],
]
crdb_vals = [
    crud["cockroachdb"]["update_1000_users_avg_ms"],
    crud["cockroachdb"]["update_1000_posts_avg_ms"],
]
x = range(len(labels2))
plt.bar([i - 0.2 for i in x], mongo_vals, width=0.4, label="Mongo")
plt.bar([i + 0.2 for i in x], crdb_vals, width=0.4, label="Cockroach")
plt.xticks(list(x), labels2)
plt.ylabel("Avg time (ms)")
plt.title("Update Operations (avg)")
plt.legend()
plt.savefig("results/combined_updates.png", dpi=150)

# 4) Deletes (total time)
plt.figure()
plt.bar(["Mongo", "Cockroach"], [
    crud["mongodb"]["delete_500_users_total_s"],
    crud["cockroachdb"]["delete_500_users_total_s"]
])
plt.ylabel("Total time (s)")
plt.title("Delete 500 Users (no posts)")
plt.savefig("results/combined_deletes.png", dpi=150)

# 5) Queries
plt.figure()
plt.bar(["Mongo", "Cockroach"], [
    qres["mongodb"]["latest20_avg_ms"],
    qres["cockroachdb"]["latest20_avg_ms"],
])
plt.ylabel("Avg time (ms)")
plt.title("Latest 20 Posts by User (avg)")
plt.savefig("results/combined_query_latest20.png", dpi=150)

plt.figure()
plt.bar(["Mongo", "Cockroach"], [
    qres["mongodb"]["range7d_avg_ms"],
    qres["cockroachdb"]["range7d_avg_ms"],
])
plt.ylabel("Avg time (ms)")
plt.title("Range Query: Last 7 Days (avg)")
plt.savefig("results/combined_query_range7d.png", dpi=150)

print("Saved combined charts to results/*.png")

