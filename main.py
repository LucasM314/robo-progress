import re
import subprocess
from pathlib import Path


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
