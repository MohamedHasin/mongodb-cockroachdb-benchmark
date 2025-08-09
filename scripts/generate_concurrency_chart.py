import json, matplotlib.pyplot as plt

with open("results/concurrency_results.json") as f:
    r = json.load(f)

# Average latency
labels = ["Mongo-10", "CRDB-10", "Mongo-50", "CRDB-50"]
avg_vals = [
    r["mongodb"]["read_threads_10"]["avg_ms"],
    r["cockroachdb"]["read_threads_10"]["avg_ms"],
    r["mongodb"]["read_threads_50"]["avg_ms"],
    r["cockroachdb"]["read_threads_50"]["avg_ms"],
]

plt.figure()
plt.bar(labels, avg_vals)
plt.ylabel("Avg latency (ms)")
plt.title("Point Lookup Concurrency (10 vs 50 threads)")
plt.savefig("results/concurrency_avg_latency.png", dpi=150)

# Throughput
tput_vals = [
    r["mongodb"]["read_threads_10"]["throughput_qps"],
    r["cockroachdb"]["read_threads_10"]["throughput_qps"],
    r["mongodb"]["read_threads_50"]["throughput_qps"],
    r["cockroachdb"]["read_threads_50"]["throughput_qps"],
]

plt.figure()
plt.bar(labels, tput_vals)
plt.ylabel("Throughput (ops/sec)")
plt.title("Point Lookup Throughput (10 vs 50 threads)")
plt.savefig("results/concurrency_throughput.png", dpi=150)

print("Saved results/concurrency_avg_latency.png and results/concurrency_throughput.png")

