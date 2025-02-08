import json
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize, SymLogNorm


# Load the benchmark JSON file
with open("results.json", "r") as file:
    data = json.load(file)

# Extract relevant benchmark data
benchmarks = data["benchmarks"]

# Parse benchmark results into a DataFrame
results = []
for bench in benchmarks:
    name_parts = bench["name"].split("/")  # Extract window and step sizes
    if len(name_parts) == 3:
        window_size = int(name_parts[1])
        step_size = int(name_parts[2])
        time_ns = bench["real_time"]  # Use real execution time in ns
        results.append((window_size, step_size, time_ns))

# Convert to DataFrame
df = pd.DataFrame(results, columns=["Window Size", "Step Size", "Time (ns)"])

# Pivot table for heatmap
df_pivot = df.pivot(index="Window Size", columns="Step Size", values="Time (ns)")

# Plot heatmap with logarithmic color scale
plt.figure(figsize=(10, 6))
sns.heatmap(df_pivot, annot=True, fmt=".1f", cmap="flare", linewidths=0.5, norm=SymLogNorm(linthresh=100))
plt.title("Execution Time Heatmap (ns) - Log Scale - Query 1")
plt.xlabel("Step Size")
plt.ylabel("Window Size")
plt.show()
