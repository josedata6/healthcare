#!/usr/bin/env python3
import os, gzip, glob
import pandas as pd

# ---------- CONFIG ----------
IN_DIR  = "/Users/jdd48774/Documents/_BANA3/healthcare/hospitaldata5"      # raw messy files
OUT_DIR = "/Users/jdd48774/Documents/_BANA3/healthcare/hospitaldata_clean" # cleaned files
FORCE_GZIP = True   # True → output .csv.gz, False → plain .csv

# Keywords we expect in a valid header row
EXPECTED_HEADERS = [
    "description", "code|1", "code|1|type", "standard_charge|gross",
    "standard_charge|discounted_cash", "payer_name", "plan_name"
]

def read_first_lines(path, n=3):
    """Return the first n lines of a file as list of strings."""
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", errors="replace", encoding="utf-8") as f:
        return [f.readline().strip() for _ in range(n)]

def should_strip_metadata(lines):
    """
    Decide if first 2 rows are metadata (not real headers).
    Returns True if we should strip them.
    """
    header_line = lines[0].lower() if lines else ""
    # If the first line already has expected headers, keep it
    if any(h in header_line for h in EXPECTED_HEADERS):
        return False
    return True

def clean_one_file(src, dst, force_gzip=False):
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    # Peek at first 3 lines
    lines = read_first_lines(src, n=3)

    if should_strip_metadata(lines):
        print(f"   → stripping top 2 rows from {os.path.basename(src)}")
        skiprows = 2
        meta1, meta2 = lines[0], lines[1]
        metadata_text = f"{meta1} | {meta2}".strip(" |")
    else:
        print(f"   → keeping first row as header in {os.path.basename(src)}")
        skiprows = 0
        metadata_text = ""

    # Read CSV with pandas
    read_kwargs = dict(
        dtype=str,
        sep=None, engine="python", quotechar='"',
        on_bad_lines="skip", skipinitialspace=True, keep_default_na=False,
        skiprows=skiprows
    )
    df = pd.read_csv(src, **read_kwargs).fillna("")
    df["metadata"] = metadata_text

    # Write cleaned file
    if force_gzip or dst.endswith(".gz"):
        if not dst.endswith(".gz"):
            dst += ".gz"
        with gzip.open(dst, "wt", encoding="utf-8") as out:
            df.to_csv(out, index=False)
    else:
        df.to_csv(dst, index=False)

    return metadata_text

def main():
    files = []
    for pat in ("*.csv", "*.csv.gz"):
        files.extend(glob.glob(os.path.join(IN_DIR, pat)))

    if not files:
        print(f"  No files found in {IN_DIR}")
        return

    print(f"Found {len(files)} file(s). Cleaning into {OUT_DIR} …")

    for src in sorted(files):
        rel = os.path.relpath(src, IN_DIR)
        dst = os.path.join(OUT_DIR, rel)
        try:
            md = clean_one_file(src, dst, force_gzip=FORCE_GZIP)
            print(f"✓ {os.path.basename(src)} → {os.path.basename(dst)} | metadata saved: {md[:50]}...")
        except Exception as e:
            print(f"✗ {src}: {e}")

    print("Cleaning complete. Use OUT_DIR with your loader.")

if __name__ == "__main__":
    main()
