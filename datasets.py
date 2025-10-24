#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = ["requests", "lxml", "humanize", "rich"]
# ///

import sys
import json
import hashlib
import os
import subprocess
import shutil
import asyncio
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from lxml import etree
import humanize
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm
from rich.progress import Progress, TaskID

BASE_URL_RAW = "https://openproblems-data.s3.amazonaws.com/"
BASE_URL_OPENPROBLEMS = "https://openproblems.bio/"

TASKS = [
    "batch_integration",
    "cell_cell_communication_source_target",
    "cell_cell_communication_ligand_target",
    "denoising",
    "dimensionality_reduction",
    # "foundation_models",
    "label_projection",
    "matching_modalities",
    "perturbation_prediction",
    "predict_modality",
    "spatial_decomposition",
    "spatially_variable_genes",
]

# Map task names to their S3 paths (for tasks where they differ)
TASK_TO_S3_PATH = {
    "cell_cell_communication_source_target": "cell_cell_communication",
    "cell_cell_communication_ligand_target": "cell_cell_communication",
}


def get_s3_task_path(task):
    """Get the S3 path for a task, handling special cases where task name != S3 path."""
    return TASK_TO_S3_PATH.get(task, task)


def list_datasets(task, dataset_name=None):
    """List all datasets for a given task, or files in a specific dataset."""
    # Try cache first for dataset listings
    if dataset_name is None:
        cache_path = get_cache_path(task)
        cached = load_cache(cache_path)
        if cached:
            return cached

    # Get the S3 path for this task (may differ from task name)
    s3_task = get_s3_task_path(task)

    params = {
        "list-type": "2",
        "prefix": f"resources/{s3_task}/datasets/",
        "max-keys": "1000",
    }

    response = requests.get(BASE_URL_RAW, params=params)
    response.raise_for_status()

    root = etree.fromstring(response.content)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    if dataset_name is None:
        # Return unique dataset names (directories) with size aggregation and file count
        dataset_info = {}
        for content in root.findall(".//s3:Contents", ns):
            key_elem = content.find("s3:Key", ns)
            size_elem = content.find("s3:Size", ns)
            if key_elem is not None:
                key = key_elem.text
                if not key.endswith("/"):  # Skip directories
                    # Extract dataset path: everything between datasets/ and filename
                    prefix = f"resources/{s3_task}/datasets/"
                    if key.startswith(prefix):
                        relative_path = key[len(prefix) :]
                        # Get directory path (everything except filename)
                        dataset_path = "/".join(relative_path.split("/")[:-1])
                        if dataset_path:  # Only add non-empty paths
                            size = int(size_elem.text) if size_elem is not None else 0
                            if dataset_path in dataset_info:
                                dataset_info[dataset_path]["size"] += size
                                dataset_info[dataset_path]["file_count"] += 1
                            else:
                                dataset_info[dataset_path] = {
                                    "size": size,
                                    "file_count": 1,
                                }

        # Convert to list with human-readable sizes
        result = []
        for dataset_path in sorted(dataset_info.keys()):
            info = dataset_info[dataset_path]
            result.append(
                {
                    "name": dataset_path,
                    "size": info["size"],
                    "size_human": humanize.naturalsize(info["size"]),
                    "file_count": info["file_count"],
                }
            )

        # Save to cache
        if dataset_name is None:
            save_cache(cache_path, result)

        return result
    else:
        # Return all files for the specific dataset
        files = []
        dataset_prefix = f"resources/{s3_task}/datasets/{dataset_name}/"

        for content in root.findall(".//s3:Contents", ns):
            key_elem = content.find("s3:Key", ns)
            size_elem = content.find("s3:Size", ns)
            etag_elem = content.find("s3:ETag", ns)

            if key_elem is not None:
                key = key_elem.text
                if key.startswith(dataset_prefix) and not key.endswith("/"):
                    size = int(size_elem.text) if size_elem is not None else 0
                    # Remove quotes from ETag
                    etag = etag_elem.text.strip('"') if etag_elem is not None else ""

                    # Extract filename
                    filename = key.split("/")[-1]

                    files.append(
                        {"name": filename, "key": key, "size": size, "md5": etag}
                    )

        return files


def calculate_md5(filepath):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def check_aria2():
    """Check if aria2c is available."""
    return shutil.which("aria2c") is not None


def fetch_file_aria2(download_url, local_path, expected_md5, expected_size):
    """Download a file using aria2c with resume and parallel connections."""
    if not check_aria2():
        print("Error: aria2c not found. Please install aria2.", file=sys.stderr)
        print("Ubuntu/Debian: sudo apt install aria2", file=sys.stderr)
        print("macOS: brew install aria2", file=sys.stderr)
        return False

    # Check if expected_md5 is a valid MD5 hash (32 hex characters)
    # S3 ETags for multipart uploads have format: <hash>-<partcount>
    # which aria2c doesn't support
    is_valid_md5 = (
        expected_md5
        and len(expected_md5) == 32
        and all(c in "0123456789abcdef" for c in expected_md5.lower())
    )

    # Prepare aria2c command
    cmd = [
        "aria2c",
        "--continue=true",  # Resume partial downloads
        "--max-connection-per-server=8",  # Parallel connections per file
        "--split=8",  # Split file into segments
        "--file-allocation=none",  # Don't pre-allocate space (fixes progress)
        "--auto-file-renaming=false",  # Don't rename on conflict
        "--allow-overwrite=true",  # Overwrite existing files
        "--show-console-readout=true",  # Show native aria2 progress
        "--console-log-level=notice",  # Show download progress
        "--human-readable=true",  # Human readable sizes in progress
        "--download-result=hide",  # Hide summary at end
    ]

    # Only add checksum if it's a valid MD5
    if is_valid_md5:
        cmd.append(f"--checksum=md5={expected_md5}")
    else:
        print(
            f"Note: Checksum '{expected_md5}' is not a simple MD5 (likely multipart upload). Skipping verification.",
            file=sys.stderr,
        )

    cmd.extend(
        [
            f"--dir={local_path.parent}",  # Download directory
            f"--out={local_path.name}",  # Output filename
            download_url,
        ]
    )

    try:
        print(f"Downloading: {local_path.name}", file=sys.stderr)
        print(f"Expected size: {humanize.naturalsize(expected_size)}", file=sys.stderr)

        # Run aria2c with its native progress display
        result = subprocess.run(cmd)

        if result.returncode == 0:
            # Verify size
            actual_size = local_path.stat().st_size
            if actual_size != expected_size:
                print(
                    f"✗ Size mismatch. Expected {expected_size}, got {actual_size}",
                    file=sys.stderr,
                )
                return False

            # If we didn't verify with aria2c and have a valid MD5, verify manually
            if not is_valid_md5 and expected_md5:
                print(
                    f"Skipping manual MD5 verification for non-standard ETag",
                    file=sys.stderr,
                )

            print(
                f"✓ Successfully downloaded: {local_path.name}",
                file=sys.stderr,
            )
            return True
        else:
            print(
                f"✗ aria2c failed with exit code {result.returncode}", file=sys.stderr
            )
            return False

    except Exception as e:
        print(f"Error running aria2c: {e}", file=sys.stderr)
        return False


def fetch_file_fallback(download_url, local_path, expected_md5, expected_size):
    """Fallback download using requests (no resume support)."""
    print(f"Downloading with fallback method: {local_path.name}", file=sys.stderr)
    print(
        "Note: No resume support. Consider installing aria2 for better downloads.",
        file=sys.stderr,
    )

    try:
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        # Download with progress indication
        total_size = int(response.headers.get("content-length", expected_size))
        downloaded = 0

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"\rProgress: {progress:.1f}%", end="", file=sys.stderr)

        print("\n", file=sys.stderr)

        # Verify file size
        actual_size = local_path.stat().st_size
        if actual_size != expected_size:
            print(
                f"✗ Size mismatch. Expected {expected_size}, got {actual_size}",
                file=sys.stderr,
            )
            return False

        # Verify MD5 hash
        print("Verifying MD5 hash...", file=sys.stderr)
        actual_md5 = calculate_md5(local_path)
        if actual_md5 != expected_md5:
            print(
                f"✗ MD5 mismatch. Expected {expected_md5}, got {actual_md5}",
                file=sys.stderr,
            )
            return False

        print(
            f"✓ Successfully downloaded and verified: {local_path.name}",
            file=sys.stderr,
        )
        return True

    except Exception as e:
        print(f"✗ Error downloading {local_path.name}: {e}", file=sys.stderr)
        if local_path.exists():
            local_path.unlink()  # Remove incomplete file
        return False


def get_cache_path(task, dataset_name=None):
    """Get cache file path for dataset info."""
    cache_dir = Path(".cache") / task
    cache_dir.mkdir(parents=True, exist_ok=True)

    if dataset_name:
        safe_name = dataset_name.replace("/", "_")
        return cache_dir / f"{safe_name}.json"
    else:
        return cache_dir / "datasets.json"


def load_cache(cache_path):
    """Load cached dataset info."""
    if cache_path.exists():
        try:
            with open(cache_path) as f:
                data = json.load(f)
                # Check if cache is less than 1 hour old
                if time.time() - data.get("timestamp", 0) < 3600:
                    return data.get("datasets")
        except:
            pass
    return None


def save_cache(cache_path, datasets):
    """Save dataset info to cache."""
    try:
        cache_data = {"timestamp": time.time(), "datasets": datasets}
        with open(cache_path, "w") as f:
            json.dump(cache_data, f)
    except:
        pass  # Ignore cache errors


def fetch_task(task, max_workers=8):
    """Download all datasets for a task with confirmation."""
    # Get list of datasets for the task
    datasets = list_datasets(task)

    if not datasets:
        print(f"No datasets found for task '{task}'", file=sys.stderr)
        return False

    # Calculate totals
    total_size = sum(d["size"] for d in datasets)
    total_datasets = len(datasets)
    total_files = sum(d["file_count"] for d in datasets)

    # Show confirmation prompt with mindful reminder
    console = Console()
    console.print(f"\n[bold red]BENCHMARK-LEVEL DOWNLOAD[/bold red]")
    console.print(f"[bold]Task:[/bold] {task}")
    console.print(f"[bold]Datasets:[/bold] {total_datasets}")
    console.print(f"[bold]Total files:[/bold] {total_files}")
    console.print(f"[bold]Total size:[/bold] {humanize.naturalsize(total_size)}")

    console.print(f"\n[yellow]⚠️  Please be mindful of bandwidth and storage:[/yellow]")
    console.print(
        f"[dim]• This will download {humanize.naturalsize(total_size)} of research data[/dim]"
    )
    console.print(
        f"[dim]• Consider downloading individual datasets if you don't need everything[/dim]"
    )
    console.print(f"[dim]• Downloads will resume if interrupted[/dim]")

    console.print(f"\n[bold red]Type 'yes I am sure' to proceed:[/bold red]")
    response = input().strip()

    if response != "yes I am sure":
        print(
            "Download cancelled. You must type 'yes I am sure' to proceed.",
            file=sys.stderr,
        )
        return False

    # Download all datasets
    success_count = 0
    for i, dataset_info in enumerate(datasets, 1):
        console.print(
            f"\n[bold]Dataset {i}/{total_datasets}:[/bold] {dataset_info['name']}"
        )
        console.print(
            f"[dim]Size: {dataset_info['size_human']}, Files: {dataset_info['file_count']}[/dim]"
        )

        success = fetch_entire_dataset(
            task, dataset_info["name"], skip_confirmation=True, max_workers=max_workers
        )
        if success:
            success_count += 1
        else:
            console.print(
                f"[red]Failed to download dataset {dataset_info['name']}[/red]"
            )

    # Final summary
    console.print(f"\n[bold]TASK DOWNLOAD COMPLETE[/bold]")
    console.print(f"Successfully downloaded: {success_count}/{total_datasets} datasets")

    if success_count == total_datasets:
        console.print(
            f"[green]✓ All datasets for '{task}' downloaded successfully![/green]"
        )
        return True
    else:
        console.print(
            f"[red]✗ {total_datasets - success_count} datasets failed to download[/red]"
        )
        return False


def fetch_dataset(task, dataset_name, filename=None, max_workers=8):
    """Download and verify files from a dataset."""
    if filename:
        # Download single file
        return fetch_single_file(task, dataset_name, filename)
    else:
        # Download entire dataset (with confirmation)
        return fetch_entire_dataset(
            task, dataset_name, skip_confirmation=False, max_workers=max_workers
        )


def fetch_single_file(task, dataset_name, filename):
    """Download and verify a specific file from a dataset."""
    # Get the list of files for the specific dataset
    files = list_datasets(task, dataset_name)

    # Find the file by name
    target_file = None
    for file_info in files:
        if file_info["name"] == filename:
            target_file = file_info
            break

    if target_file is None:
        print(
            f"Error: File '{filename}' not found in dataset '{dataset_name}' of task '{task}'",
            file=sys.stderr,
        )
        return False

    # Create dataset directory in root
    dataset_dir = Path("datasets") / dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)
    local_path = dataset_dir / filename

    # Download URL
    download_url = BASE_URL_RAW + target_file["key"]

    # Try aria2c first, fallback to requests
    if check_aria2():
        success = fetch_file_aria2(
            download_url, local_path, target_file["md5"], target_file["size"]
        )
    else:
        success = fetch_file_fallback(
            download_url, local_path, target_file["md5"], target_file["size"]
        )

    if success:
        print(f"Saved to: {local_path}", file=sys.stderr)

    return success


def download_single_file_worker(args):
    """Worker function to download a single file. Returns (file_info, success, local_path)."""
    file_info, dataset_dir, file_index, total_files, lock = args

    local_path = dataset_dir / file_info["name"]
    download_url = BASE_URL_RAW + file_info["key"]

    # Skip if already exists and has correct size
    if local_path.exists() and local_path.stat().st_size == file_info["size"]:
        with lock:
            print(
                f"[{file_index}/{total_files}] Skipping {file_info['name']} (already exists)",
                file=sys.stderr,
            )
        return (file_info, True, local_path)

    with lock:
        print(
            f"[{file_index}/{total_files}] Downloading {file_info['name']}",
            file=sys.stderr,
        )

    # Try aria2c first, fallback to requests
    if check_aria2():
        success = fetch_file_aria2(
            download_url, local_path, file_info["md5"], file_info["size"]
        )
    else:
        success = fetch_file_fallback(
            download_url, local_path, file_info["md5"], file_info["size"]
        )

    if not success:
        with lock:
            print(f"Failed to download {file_info['name']}", file=sys.stderr)

    return (file_info, success, local_path)


def fetch_entire_dataset(task, dataset_name, skip_confirmation=False, max_workers=8):
    """Download all files in a dataset with optional confirmation."""
    # Get file list (use cache if available)
    cache_path = get_cache_path(task, dataset_name)
    files = load_cache(cache_path)

    if files is None:
        print(f"Fetching file list for {dataset_name}...", file=sys.stderr)
        files = list_datasets(task, dataset_name)
        save_cache(cache_path, files)

    if not files:
        print(f"No files found in dataset '{dataset_name}'", file=sys.stderr)
        return False

    # Calculate totals
    total_size = sum(f["size"] for f in files)
    file_count = len(files)

    # Initialize console for all cases
    console = Console()

    # Show confirmation prompt only if not skipping
    if not skip_confirmation:
        console.print(f"\n[bold]Dataset:[/bold] {dataset_name}")
        console.print(f"[bold]Files:[/bold] {file_count}")
        console.print(f"[bold]Total size:[/bold] {humanize.naturalsize(total_size)}")

    # Check existing files
    dataset_dir = Path("datasets") / dataset_name
    existing_files = []
    missing_size = 0

    if dataset_dir.exists():
        for file_info in files:
            local_path = dataset_dir / file_info["name"]
            if not local_path.exists():
                missing_size += file_info["size"]
            else:
                existing_files.append(file_info["name"])
    else:
        missing_size = total_size

    if not skip_confirmation:
        if existing_files:
            console.print(f"[dim]Already downloaded: {len(existing_files)} files[/dim]")

        if missing_size > 0:
            console.print(
                f"[yellow]To download: {humanize.naturalsize(missing_size)}[/yellow]"
            )

            if not Confirm.ask(f"\nProceed with download?"):
                print("Download cancelled.", file=sys.stderr)
                return False
        else:
            console.print("[green]All files already downloaded![/green]")
            return True

    # Create directory
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Download files in parallel
    success_count = 0
    total_files = len(files)

    # Create a lock for thread-safe printing
    print_lock = threading.Lock()

    # Prepare arguments for worker function
    download_args = [
        (file_info, dataset_dir, i, total_files, print_lock)
        for i, file_info in enumerate(files, 1)
    ]

    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        futures = {
            executor.submit(download_single_file_worker, args): args[0]
            for args in download_args
        }

        # Process completed downloads
        for future in as_completed(futures):
            file_info, success, local_path = future.result()
            if success:
                success_count += 1

    console.print(f"\n[bold]Completed:[/bold] {success_count}/{total_files} files")

    if success_count == total_files:
        console.print(
            f"[green]✓ Dataset '{dataset_name}' downloaded successfully![/green]"
        )
        return True
    else:
        console.print(
            f"[red]✗ {total_files - success_count} files failed to download[/red]"
        )
        return False


def print_usage():
    print("Usage:")
    print("  python datasets.py list [--json]")
    print("  python datasets.py list <task> [--json]")
    print("  python datasets.py list <task> <dataset> [--json]")
    print("  python datasets.py fetch <task> [--workers=N]")
    print("  python datasets.py fetch <task> <dataset> [<filename>] [--workers=N]")
    print()
    print("Options:")
    print("  --json           Output in JSON format (for list command)")
    print("  --workers=N      Number of parallel download workers (default: 8)")
    print()
    print("Examples:")
    print("  # List all available tasks")
    print("  python datasets.py list")
    print()
    print("  # List all datasets for a task")
    print("  python datasets.py list denoising")
    print()
    print("  # List files in a specific dataset")
    print("  python datasets.py list denoising cellxgene_census/dkd/log_cp10k")
    print()
    print("  # Download ALL datasets for a task (with confirmation)")
    print("  python datasets.py fetch denoising")
    print()
    print("  # Download single dataset with 4 parallel workers")
    print(
        "  python datasets.py fetch denoising cellxgene_census/dkd/log_cp10k --workers=4"
    )
    print()
    print("  # Download single file from dataset")
    print(
        "  python datasets.py fetch denoising cellxgene_census/dkd/log_cp10k state.yaml"
    )
    print()
    print("Available tasks:")
    for task in TASKS:
        print(f"  {task}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    command = sys.argv[1]

    if command == "list":
        # Parse arguments and check for --json flag
        args = [arg for arg in sys.argv[2:] if arg != "--json"]
        json_output = "--json" in sys.argv

        if len(args) == 0:
            # Show available tasks
            if json_output:
                print(json.dumps(TASKS, indent=2))
            else:
                console = Console()
                table = Table(title="Available Tasks")
                table.add_column("Task", style="cyan")

                for task in TASKS:
                    table.add_row(task)

                console.print(table)
            sys.exit(0)
        elif len(args) not in [1, 2]:
            print(
                "Usage: python datasets.py list [<task>] [<dataset>] [--json]",
                file=sys.stderr,
            )
            sys.exit(1)

        task = args[0]
        dataset = args[1] if len(args) == 2 else None

        if task not in TASKS:
            print(f"Error: Unknown task '{task}'", file=sys.stderr)
            print("Available tasks:", ", ".join(TASKS), file=sys.stderr)
            sys.exit(1)

        try:
            result = list_datasets(task, dataset)

            if json_output:
                print(json.dumps(result, indent=2))
            else:
                console = Console()

                if dataset is None:
                    # Show dataset table with sizes
                    table = Table(title=f"Datasets for {task}")
                    table.add_column("Dataset", style="cyan", no_wrap=True)
                    table.add_column("Size", justify="right", style="green")
                    table.add_column("Files", justify="right", style="yellow")

                    total_size = 0
                    for item in result:
                        table.add_row(
                            item["name"], item["size_human"], str(item["file_count"])
                        )
                        total_size += item["size"]

                    console.print(table)
                    console.print(
                        f"\n[bold]Total: {humanize.naturalsize(total_size)}[/bold]"
                    )
                else:
                    # Show file table
                    table = Table(title=f"Files in {task}/{dataset}")
                    table.add_column("File", style="cyan")
                    table.add_column("Size", justify="right", style="green")
                    table.add_column("MD5", style="dim", no_wrap=True)

                    total_size = 0
                    for file_info in result:
                        table.add_row(
                            file_info["name"],
                            humanize.naturalsize(file_info["size"]),
                            file_info["md5"][:8] + "..."
                            if len(file_info["md5"]) > 8
                            else file_info["md5"],
                        )
                        total_size += file_info["size"]

                    console.print(table)
                    console.print(
                        f"\n[bold]Total: {humanize.naturalsize(total_size)}[/bold]"
                    )

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    elif command == "fetch":
        # Parse arguments and check for --workers flag
        args = [arg for arg in sys.argv[2:] if not arg.startswith("--")]

        # Extract workers count
        max_workers = 8  # default
        for arg in sys.argv[2:]:
            if arg.startswith("--workers="):
                try:
                    max_workers = int(arg.split("=")[1])
                    if max_workers < 1:
                        print("Error: --workers must be at least 1", file=sys.stderr)
                        sys.exit(1)
                except ValueError:
                    print("Error: --workers must be a number", file=sys.stderr)
                    sys.exit(1)

        if len(args) not in [1, 2, 3]:
            print(
                "Usage: python datasets.py fetch <task> [<dataset>] [<filename>] [--workers=N]",
                file=sys.stderr,
            )
            sys.exit(1)

        task = args[0]
        dataset = args[1] if len(args) >= 2 else None
        filename = args[2] if len(args) == 3 else None

        if task not in TASKS:
            print(f"Error: Unknown task '{task}'", file=sys.stderr)
            print("Available tasks:", ", ".join(TASKS), file=sys.stderr)
            sys.exit(1)

        try:
            if dataset is None:
                # Download entire task
                success = fetch_task(task, max_workers=max_workers)
            else:
                # Download specific dataset/file
                success = fetch_dataset(
                    task, dataset, filename, max_workers=max_workers
                )
            sys.exit(0 if success else 1)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    else:
        print(f"Error: Unknown command '{command}'", file=sys.stderr)
        print_usage()
        sys.exit(1)
