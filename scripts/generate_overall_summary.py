import json, csv
import matplotlib.pyplot as plt
from pathlib import Path

res = Path("results")
naive = json.loads((res/"performance_results.json").read_text())
batched = json.loads((res/"performance_results_batched.json").read_text())
crud = json.loads((res/"crud_results.json").read_text())
qres = json.loads((res/"query_results.json").read_text())

# Pick fair values:
# - Inserts: Mongo (bulk) from naive; Cockroach from *batched*
mongo_insert_1k = naive["mongodb"]["insert_1000_users"]
mongo_insert_5k = naive["mongodb"]["insert_5000_posts"]
crdb_insert_1k  = batched["cockroachdb_batched"]["insert_1000_users"]
crdb_insert_5k  = batched["cockroachdb_batched"]["insert_5000_posts"]

# Updates: average of username + post-content update avg times (ms)
mongo_upd_avg = (crud["mongodb"]["update_1000_users_avg_ms"] + crud["mongodb"]["update_1000_posts_avg_ms"]) / 2.0
crdb_upd_avg  = (crud["cockroachdb"]["update_1000_users_avg_ms"] + crud["cockroachdb"]["update_1000_posts_avg_ms"]) / 2.0

# Deletes: total time to delete 500 users (s)
mongo_del = crud["mongodb"]["delete_500_users_total_s"]
crdb_del  = crud["cockroachdb"]["delete_500_users_total_s"]

# Queries: avg latency (ms)
mongo_latest20 = qres["mongodb"]["latest20_avg_ms"]
crdb_latest20  = qres["cockroachdb"]["latest20_avg_ms"]
mongo_range7d  = qres["mongodb"]["range7d_avg_ms"]
crdb_range7d   = qres["cockroachdb"]["range7d_avg_ms"]

rows = [
    ["Insert 1000 users (s)", mongo_insert_1k, crdb_insert_1k],
    ["Insert 5000 posts (s)", mongo_insert_5k, crdb_insert_5k],
    ["Update avg (ms/op)",     mongo_upd_avg,  crdb_upd_avg],
    ["Delete 500 users (s)",   mongo_del,      crdb_del],
    ["Latest-20 (ms)",         mongo_latest20, crdb_latest20],
    ["Range 7 days (ms)",      mongo_range7d,  crdb_range7d],
]

# Write CSV + Markdown
(res/"overall_summary.csv").write_text(
    "Metric,MongoDB,CockroachDB (batched)\n" +
    "\n".join(f"{m},{v1:.6f},{v2:.6f}" for m,v1,v2 in rows)
)

md_lines = ["| Metric | MongoDB | CockroachDB (batched) |",
            "|---|---:|---:|"]
for m,v1,v2 in rows:
    md_lines.append(f"| {m} | {v1:.3f} | {v2:.3f} |")
(res/"overall_summary.md").write_text("\n".join(md_lines))

# One compact bar chart
labels = [r[0] for r in rows]
mongo_vals = [r[1] for r in rows]
crdb_vals  = [r[2] for r in rows]

x = range(len(labels))
plt.figure(figsize=(10,5))
plt.bar([i-0.2 for i in x], mongo_vals, width=0.4, label="MongoDB")
plt.bar([i+0.2 for i in x], crdb_vals,  width=0.4, label="Cockroach (batched)")
plt.xticks(list(x), labels, rotation=20, ha="right")
plt.ylabel("Value (units on x-label)")
plt.title("Overall Comparison (lower is better)")
plt.legend()
plt.tight_layout()
plt.savefig(res/"overall_summary.png", dpi=150)
print("Wrote results/overall_summary.csv, results/overall_summary.md, results/overall_summary.png")

