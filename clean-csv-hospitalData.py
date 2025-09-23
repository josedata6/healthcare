#!/usr/bin/env python3
import os, gzip, glob
import pandas as pd

# ---------- CONFIG (edit these paths) ----------
IN_DIR  = "/Users/jdd48774/Documents/_BANA3/healthcare/hospitaldata5"      # raw messy files
OUT_DIR = "/Users/jdd48774/Documents/_BANA3/healthcare/hospitaldata_clean" # cleaned files
FORCE_GZIP = True   # set to True if you want all output as .csv.gz

# ---------- Helpers ----------
def read_first_two_lines(path):
    """Return the first two lines and the rest of the file as text."""
    if path.endswith(".gz"):
        with gzip.open(path, "rt", errors="replace") as f:
            l1 = f.readline().strip()
            l2 = f.readline().strip()
            rest = f.read()
        return l1, l2, rest
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            l1 = f.readline().strip()
            l2 = f.readline().strip()
            rest = f.read()
        return l1, l2, rest

def clean_one_file(src, dst, force_gzip=False):
    """Clean one CSV: strip top 2 rows, add as metadata column, write to dst."""
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    meta1, meta2, _ = read_first_two_lines(src)
    metadata_text = f"{meta1} | {meta2}".strip(" |")

    # Read file with pandas, skipping first 2 rows
    read_kwargs = dict(
        dtype=str,
        sep=None,              # auto-detect delimiter
        engine="python",
        quotechar='"',
        on_bad_lines="skip",
        skipinitialspace=True,
        keep_default_na=False,
        skiprows=2             # header starts on 3rd row
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
            print(f"✓ {os.path.basename(src)} → {os.path.basename(dst)} | metadata captured: {md[:60]}...")
        except Exception as e:
            print(f"✗ {src}: {e}")

    print("Cleaning complete. Point your loader at OUT_DIR.")

if __name__ == "__main__":
    main()
