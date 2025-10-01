#!/usr/bin/env python3
import os, glob, gzip, io, re
import pandas as pd

# ================== CONFIG ==================
IN_DIR  = "/Users/jdd48774/Downloads/wide-to-tall"      # raw folder
OUT_DIR = "/Users/jdd48774/Downloads/out-tall" # cleaned folder
FORCE_GZIP = True  # True -> write .csv.gz

# Pricing/structure headers we expect in a REAL header row
EXPECTED_TOKENS = {
    "description", "code|", "code|1", "code|1|type", "code|2", "code|2|type",
    "standard_charge", "standard_charge|gross", "standard_charge|discounted_cash",
    "standard_charge|negotiated_dollar", "standard_charge|negotiated_percentage",
    "standard_charge|min", "standard_charge|max",
    "payer_name", "plan_name", "billing_class", "setting", "currency",
}

# “Admin-only” tokens that frequently show up in those top 2 rows (metadata)
ADMIN_HINTS = {
    "hospital_name", "last_updated_on", "hospital_location", "hospital_address",
    "license_number|", "license_number|wa", "version",
    "to the best of its knowledge",  # attestation text
    "45 cfr 180.50", "attestation", "standard charge information"
}
# ============================================

def open_text(path):
    if path.lower().endswith(".gz"):
        return gzip.open(path, "rt", errors="replace", encoding="utf-8")
    return open(path, "r", errors="replace", encoding="utf-8")

def first_lines(path, n=3):
    with open_text(path) as f:
        return [f.readline() for _ in range(n)]

def normalize(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    # unify whitespace and separators for matching
    s = s.replace("\u00A0", " ")       # NBSP -> space
    s = s.replace("\u2007", " ")       # figure space
    s = s.replace("\u202F", " ")       # narrow NBSP
    s = re.sub(r"\s+", " ", s)
    s = s.strip().lower()
    return s

def score_header(raw_header_line: str) -> int:
    """Give a higher score if header looks like a charge file header, not admin."""
    h = normalize(raw_header_line)
    score = 0
    # reward pricing tokens
    for tok in EXPECTED_TOKENS:
        if tok in h:
            score += 2
    # penalize if it's clearly admin-y
    for tok in ADMIN_HINTS:
        if tok in h:
            score -= 1
    return score

def read_csv_pass(path, skiprows=0):
    """Read with pandas in a tolerant way (autodetect delimiter)."""
    return pd.read_csv(
        open_text(path),
        dtype=str,
        sep=None, engine="python",  # autodetect delimiter
        quotechar='"',
        on_bad_lines="skip",
        skipinitialspace=True,
        keep_default_na=False,
        skiprows=skiprows,
    ).fillna("")

def clean_one_file(src_path, out_path):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Pass A: assume first row is header
    lines = first_lines(src_path, n=3)
    line1 = lines[0] if lines else ""
    score_A = score_header(line1)

    # Pass B: strip the first 2 rows (common WA admin header + attestation)
    line3 = lines[2] if len(lines) >= 3 else ""
    score_B = score_header(line3)

    # If Pass B header looks more like a pricing header, use Pass B
    use_pass_B = (score_B > score_A)

    if use_pass_B:
        print(f" → {os.path.basename(src_path)}: using header at row 3 (stripping 2 admin rows)")
        df = read_csv_pass(src_path, skiprows=2)
        # Save the 2 stripped lines in metadata
        meta_text = " | ".join([normalize(lines[0]) if len(lines) >= 1 else "",
                                normalize(lines[1]) if len(lines) >= 2 else ""]).strip(" |")
    else:
        print(f" → {os.path.basename(src_path)}: using header at row 1")
        df = read_csv_pass(src_path, skiprows=0)
        meta_text = ""

    # Always add a metadata column (blank if none)
    df["metadata"] = meta_text

    # Write out
    if FORCE_GZIP:
        if not out_path.lower().endswith(".gz"):
            out_path = out_path + ".gz"
        with gzip.open(out_path, "wt", encoding="utf-8", newline="") as gz:
            df.to_csv(gz, index=False)
    else:
        df.to_csv(out_path, index=False)

    # Some quick feedback
    cols_preview = ", ".join(list(df.columns)[:10])
    print(f"   ✓ wrote {os.path.basename(out_path)}  | rows={len(df):,}  cols={len(df.columns)}  [{cols_preview}]")

def main():
    files = []
    for pat in ("*.csv", "*.csv.gz"):
        files.extend(glob.glob(os.path.join(IN_DIR, pat)))
    if not files:
        print(f"⚠️  No files found in {IN_DIR}")
        return

    print(f"Found {len(files)} file(s). Cleaning into {OUT_DIR} …")
    for src in sorted(files):
        rel = os.path.relpath(src, IN_DIR)
        out = os.path.join(OUT_DIR, rel)
        try:
            clean_one_file(src, out)
        except Exception as e:
            print(f"✗ {os.path.basename(src)}: {e}")

    print("✅ Cleaning complete. Point your loader at OUT_DIR.")

if __name__ == "__main__":
    main()
