import json
import matplotlib.pyplot as plt

with open("results/performance_results.json") as f:
    r = json.load(f)

labels = ["MongoDB", "CockroachDB"]

plt.figure()
plt.bar(labels, [r["mongodb"]["insert_1000_users"], r["cockroachdb"]["insert_1000_users"]])
plt.ylabel("Time (s)")
plt.title("Insert 1000 Users")
plt.savefig("results/insert_users.png", dpi=150)

plt.figure()
plt.bar(labels, [r["mongodb"]["single_query"], r["cockroachdb"]["single_query"]])
plt.ylabel("Time (ms)")
plt.title("Single User Query (Average)")
plt.savefig("results/single_query.png", dpi=150)

plt.figure()
plt.bar(labels, [r["mongodb"]["insert_5000_posts"], r["cockroachdb"]["insert_5000_posts"]])
plt.ylabel("Time (s)")
plt.title("Insert 5000 Posts")
plt.savefig("results/insert_posts.png", dpi=150)

print("Saved charts to results/*.png")

