#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Convert nested results.json files to flattened format for R-friendly processing.
Flattens metric_values, scaled_scores, and resources into top-level columns.
"""

import json
import sys
from pathlib import Path


def flatten_entry(key, entry):
    """Flatten a single result entry into a flat dictionary."""
    flat = {
        "task_id": entry["task_id"],
        "method_id": entry["method_id"],
        "dataset_id": entry["dataset_id"],
        "mean_score": entry.get("mean_score"),
        "commit_sha": entry.get("commit_sha"),
        "code_version": entry.get("code_version"),
        "submission_time": entry.get("submission_time"),
    }

    # Flatten metric_values
    if "metric_values" in entry:
        for metric, value in entry["metric_values"].items():
            flat[metric] = value

    # Flatten scaled_scores with _scaled suffix
    if "scaled_scores" in entry:
        for metric, value in entry["scaled_scores"].items():
            flat[f"{metric}_scaled"] = value

    # Flatten resources
    if "resources" in entry:
        for resource, value in entry["resources"].items():
            flat[resource] = value

    return flat


def convert_results(input_file, output_file):
    """Convert a results.json file to flattened format."""
    with open(input_file, "r") as f:
        data = json.load(f)

    flattened = []
    for key, entry in data.items():
        flat_entry = flatten_entry(key, entry)
        flattened.append(flat_entry)

    with open(output_file, "w") as f:
        json.dump(flattened, f, indent=2)

    print(f"Converted {len(flattened)} entries from {input_file} to {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: flatten_results.py <benchmark_name>")
        print("Example: flatten_results.py denoising")
        sys.exit(1)

    benchmark_name = sys.argv[1]
    input_file = Path("metadata") / benchmark_name / "results.json"

    if not input_file.exists():
        print(f"Error: {input_file} does not exist")
        sys.exit(1)

    conversion_dir = Path("conversion") / "results"
    conversion_dir.mkdir(parents=True, exist_ok=True)

    output_file = conversion_dir / f"{benchmark_name}_results_flat.json"
    convert_results(input_file, output_file)


if __name__ == "__main__":
    main()
