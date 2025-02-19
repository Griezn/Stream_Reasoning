import json
import numpy as np
import matplotlib.pyplot as plt

# Load the JSON data
file_path = "results.json"  # Update with the correct file path
with open(file_path, "r") as file:
    data = json.load(file)

# Extract benchmarks
benchmarks = data["benchmarks"]

# Organize data by query
queries = {}
for entry in benchmarks:
    name_parts = entry["name"].split("/")
    query_name = name_parts[0]
    window_size = int(name_parts[1].split("_")[0])
    metric = name_parts[1].split("_")[1]

    if query_name not in queries:
        queries[query_name] = {}

    if window_size not in queries[query_name]:
        queries[query_name][window_size] = {}

    queries[query_name][window_size][metric] = entry["real_time"]

# Create plots
for query, results in queries.items():
    window_sizes = sorted(results.keys())
    execution_times = np.array([results[w]["mean"] for w in window_sizes])
    std_devs = np.array([results[w]["stddev"] for w in window_sizes])

    # Normalize execution times
    min_time = np.min(execution_times)
    norm_execution_times = execution_times / min_time
    norm_std_devs = std_devs / min_time

    # Plot
    plt.figure(figsize=(8, 5))
    plt.errorbar(window_sizes, norm_execution_times, yerr=norm_std_devs, fmt='-o', capsize=5, label=query)
    plt.xscale('log', base=2)
    plt.xlabel("Batch Size")
    plt.ylabel("Normalized Execution Time")
    plt.title(f"Execution Time vs Batch Size for {query}")
    plt.legend()
    plt.grid(True)
    plt.show()
