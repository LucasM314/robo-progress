import argparse
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    DownloadColumn,
    TimeElapsedColumn,
    TransferSpeedColumn,
)

console = Console()

ROBOCOPY_TRANSFER_FLAGS = [
    "/E",
    "/Z",
    "/MT",
    "/R:3",
    "/W:5",
    "/NJH",
    "/NJS",
    "/NDL",
    "/NC",
    "/BYTES",
    "/UNILOG+:log.txt",
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
        "--logs-dir",
        type=Path,
        default=Path(__file__).parent / "logs",
        help="Path to save the log file. Default: ./logs/",
    )
    return parser.parse_args()


def copy_folder(
    source_path: Path, destination_parent: Path, progress: Progress, logs_dir: Path
) -> dict:
    pass


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

    # Ensure destination exists
    args.destination_parent.mkdir(parents=True, exist_ok=True)

    # Setup Rich progress bar
    progress = Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        "•",
        DownloadColumn(),
        TransferSpeedColumn(),
        "•",
        TimeElapsedColumn(),
    )

    results = []
    with progress:
        for source_dir in args.source_directories:
            source_path = build_unc_path(args.source_pc, source_dir)
            if not source_path.exists():
                console.print(
                    f"[yellow]Source path ignored (not found): {source_path}[/yellow]"
                )
                continue
            result = copy_folder(
                source_path, Path(args.destination_parent), progress, args.logs_dir
            )
            results.append(result)

    # Summary
    total_bytes = sum(r["bytes_copied"] for r in results)
    total_time = sum(r["time_seconds"] for r in results)
    average_speed = (
        total_bytes / 1_048_576 / total_time if total_time > 0 else 0
    )  # MB/s
    console.print(
        f"\n[dark_olive_green3][bold]Total (GB): {(total_bytes / 1_073_741_824):.2f} | Time: "
        f"{total_time:.1f)}s ({average_speed} MB/s)[/bold][/dark_olive_green3]"
    )

    console.print(f"\nLog files saved to: {args.logs_dir.resolve()}")
