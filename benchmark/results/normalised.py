import json
import matplotlib.pyplot as plt
import numpy as np

# Load JSON data from file
file_path = "results.json"
with open(file_path, 'r') as file:
    data = json.load(file)

# Extract benchmark names and real times
benchmarks = data["benchmarks"]
names = [b["name"] for b in benchmarks]
real_times = np.array([b["real_time"] for b in benchmarks])

# Normalize times so the lowest value is 1
min_time = np.min(real_times)
normalized_times = real_times / min_time

# Plot
plt.figure(figsize=(10, 6))
plt.plot(names, normalized_times, marker='o', linestyle='-', color='skyblue', label='Normalized Execution Time')
plt.axhline(y=1, color='r', linestyle='dotted', label='Baseline (1.0)')
plt.xlabel("Benchmark")
plt.ylabel("Normalized Execution Time")
plt.title("Normalized Execution Time of Benchmarks")
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
