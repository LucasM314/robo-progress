import argparse
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path, WindowsPath
from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)
from concurrent.futures import ThreadPoolExecutor
import time

from rich.text import Text

console = Console()

ROBOCOPY_BASE_FLAGS = [
    # "/E",  -> /E or /MIR will be added dynamically
    "/Z",  # Restartable mode
    # "/MT", -> Will be added dynamically
    "/R:3",  # Retries on failed copies
    "/W:5",  # Wait time between retries
    "/NJH",  # No Job Header
    "/NJS",  # No Job Summary
    "/NDL",  # No Directory List
    "/NC",  # No Class
    "/BYTES",  # Output sizes in bytes
    # "UNILOG+:{log_file}", -> log file path will be added dynamically
    "/TEE",  # duplicate log output to console (stdout) for the progress bar to read
]

# Regular expressions to parse robocopy output
FILE_LINE_RE = re.compile(r"^\s*(?P<size>\d+)\s+(?P<path>.+)$")  # "bytes<spaces>path"
PERCENTAGE_RE = re.compile(
    r"^\s*(?P<percentage>\d{1,2}(?:\.\d)?|100)\s*%$"
)  # "NN%" or "NN.N%" for decimal percentages


def build_unc_path(source_pc, path):
    """
    Build a UNC path for a given PC and local path.
    """
    return Path(r"\\") / source_pc / path


def get_unc_share_name(path: str | Path) -> str:
    r"""
    Extract the share name from a UNC path.
    E.g. \\PC-NAME\Share\Folder -> Share
    """
    # For a UNC path like '//PC-NAME/Folder/file', path.drive is '\\PC-NAME\Folder'
    return Path(path).drive.split(os.sep)[-1]

def smart_leaf_name(path: Path) -> str:
    """Return the last path component; if empty (UNC share root), fall back to share name."""
    return path.name if path.name else get_unc_share_name(path)


def get_stats(directory_path: str | Path) -> dict:
    """
    Estimate the total size (in bytes) and file count of a given directory using robocopy in list mode (/L).
    This method is much faster than os.walk for large directory trees.

    Args:
        directory_path (str | Path): The path to the directory to analyze.

    Returns:
        dict: A dictionary with keys 'size_bytes' and 'file_count'.

    Raises:
        ValueError: If robocopy output cannot be parsed for size or file count.
    """
    cmd = [
        "robocopy",
        str(directory_path),
        "NUL",
        "/L",
        "/E",
        "/BYTES",
        "/NFL",
        "/NDL",
        "/NJH",
        "/NC",
        "/NS",
        "/NP",
        "/XJ",
        "/R:0",
        "/W:0",
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="ignore",
        check=False,
    )
    output = result.stdout or ""

    # Extract the last reported values in case of multiple summary blocks
    size_matches = re.findall(
        r"(?:Bytes|Octets)\s*:\s*([\d,.]+)", output, re.IGNORECASE
    )
    file_matches = re.findall(
        r"(?:Files|Fichiers)\s*:\s*([\d,.]+)", output, re.IGNORECASE
    )

    if not size_matches or not file_matches:
        raise ValueError(f"Could not parse robocopy output:\n{output}")

    try:
        size_bytes = int(re.sub(r"\D", "", size_matches[-1]))
        file_count = int(re.sub(r"\D", "", file_matches[-1]))
    except Exception as exc:
        raise ValueError(f"Failed to convert robocopy output to int: {exc}\n{output}")

    return {"size_bytes": size_bytes, "file_count": file_count}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Copy folders from a source PC to a destination using robocopy with progress."
    )
    parser.add_argument(
        "source_pc",
        help="The hostname of the source PC (e.g. DESKTOP-72UJJRL).",
    )
    parser.add_argument(
        "source_directories",
        nargs="+",
        help="One or more directories to copy from the source PC.",
    )
    parser.add_argument(
        "destination_parent",
        type=Path,
        help="Destination parent folder (local path).",
    )
    parser.add_argument(
        "--mirror",
        action="store_true",
        help="Use /MIR instead of /E (WARNING: deletes files not present in source).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=16,
        help="Number of threads for robocopy (/MT option). Default: 16",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=3,
        help="Number of folders to copy in parallel. Default: 3",
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path(__file__).parent / "logs",
        help="Path to save the log file. Default: ./logs/",
    )
    return parser.parse_args()


def copy_folder(
    source_path: Path,
    destination_parent: Path,
    progress: Progress,
    logs_dir: Path,
    task_id,
    mirror_flag: bool,
    threads: int,
) -> dict:
    """
    Copies a single folder using robocopy, updating a Rich progress bar in real-time.
    """
    source_path_name = smart_leaf_name(source_path)

    try:
        stats = get_stats(source_path)
        total_bytes = stats["size_bytes"]
        progress.update(task_id, total=total_bytes)
        # Reset the clock of the task.
        # See https://rich.readthedocs.io/en/stable/reference/progress.html#rich.progress.Progress.reset
        progress.reset(task_id)  # reset starts the task by default
    except ValueError as e:
        progress.update(
            task_id,
            description=f"[bold red]Error getting stats for {source_path_name}[/bold red]",
            completed=1,
        )
        console.log(f"[red]Skipping {source_path_name}: {e}[/red]")
        return {"bytes_copied": 0, "time_seconds": 0}

    destination_path = destination_parent / source_path_name
    destination_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = logs_dir / f"{source_path_name}_{timestamp}.log"
    logs_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "robocopy",
        str(source_path),
        str(destination_path),
        "/MIR" if mirror_flag else "/E",
        f"/MT:{threads}",
        f"/UNILOG+:{log_file_path}",
        *ROBOCOPY_BASE_FLAGS,
    ]

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, text=True
    )

    current_file_size = 0
    current_file_done = 0

    for line in iter(process.stdout.readline, ""):
        line = line.strip()
        if not line:
            continue

        if match := FILE_LINE_RE.match(line):
            current_file_size = int(match.group("size"))
            current_file_done = 0
            continue

        if match := PERCENTAGE_RE.match(line):
            percentage = float(match.group("percentage"))
            new_done = int(current_file_size * (percentage / 100.0))
            delta = new_done - current_file_done
            if delta > 0:
                progress.advance(task_id, delta)
                current_file_done = new_done

    process.wait()

    # Ensure the progress bar reaches 100%
    # progress.update(task_id, completed=total_bytes)
    progress.stop_task(task_id)
    progress.update(task_id, description=f"[green]{source_path_name}[/green]")

    return {
        "bytes_copied": total_bytes,
        "time_seconds": progress.tasks[task_id].elapsed,
    }


class HideOnCompleteSpeedColumn(TransferSpeedColumn):
    """A TransferSpeedColumn that hides itself when the task is finished."""
    def render(self, task) -> Text:
        if task.finished:
            return Text("")  # hide when complete
        return super().render(task)

def main():
    args = parse_args()

    # Confirm /MIR if enabled
    if args.mirror:
        console.print("[red]WARNING: --mirror enabled![/red]")
        console.print(
            "This will DELETE files in destination that are missing in source."
        )
        confirm = input("Do you really want to continue? (y/N): ").lower()
        if confirm not in ("y", "yes"):
            console.print("[yellow]Operation cancelled by user.[/yellow]")
            sys.exit(0)

    # Setup the Rich progress bar used for each folder.
    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None, complete_style="blue", pulse_style="white"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        "•",
        DownloadColumn(),
        HideOnCompleteSpeedColumn(),
        "•",
        TimeElapsedColumn(),
    )

    # Build and validate source paths.
    source_paths = []
    for source_dir in args.source_directories:
        source_path = build_unc_path(args.source_pc, source_dir)
        if not source_path.exists() or not source_path.is_dir():
            console.print(
                f"[yellow]Source path ignored (not found or not a directory): {source_path}[/yellow]"
            )
            continue
        source_paths.append(source_path)
    if not source_paths:
        console.print("[red]No valid source directories found. Exiting.[/red]")
        sys.exit(1)

    results = []
    with progress:
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            # Create all progress tasks first, so they appear in the UI from the start
            tasks = {
                executor.submit(
                    copy_folder,
                    source_path,
                    args.destination_parent,
                    progress,
                    args.logs_dir,
                    task_id,
                    args.mirror,
                    args.threads,
                ): source_path
                for source_path, task_id in [
                    (p, progress.add_task(f"{smart_leaf_name(p)}", total=None))
                    for p in source_paths
                ]
            }

            for future in tasks:
                # This will block until a future is complete, then loop
                result = future.result()
                results.append(result)

    # Summary
    total_bytes = sum(r["bytes_copied"] for r in results)
    total_time = max(
        (r["time_seconds"] for r in results), default=0
    )  # Use max time for parallel runs
    average_speed = (
        total_bytes / 1_048_576 / total_time if total_time > 0 else 0
    )  # MB/s
    console.print(
        f"\n[dark_olive_green3][bold]Total Copied: {(total_bytes / 1_073_741_824):.2f} GB | "
        f"Total Time: {total_time:.1f}s | "
        f"Average Speed: {average_speed:.2f} MB/s[/bold][/dark_olive_green3]"
    )

    console.print(f"\nLog files saved to: {args.logs_dir.resolve()}")


if __name__ == "__main__":
    main()
