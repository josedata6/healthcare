import os
import re
import io
import csv
import pandas as pd

# ===================== CONFIG =====================
ROOT_PATH = "/Volumes/Lexar/Raw_Data"  # your USB root
RECURSIVE = True                       # scan subfolders
EXPORT_SUMMARY = True                  # save summary CSV
SUMMARY_NAME = "file_summary.csv"      # saved into ROOT_PATH
SAMPLE_ROWS = 5000                     # read up to this many rows for speed
MIN_WIDE_SUFFIX_GROUP = 3              # >=3 columns sharing a base+suffix => wide signal
TREAT_TXT_AS_CSV = True                # try to parse .txt as delimited text
TRY_SHEETS = True                      # read all visible sheets in Excel; classify each

# encodings & CSV read fallbacks
ENCODINGS = ["utf-8-sig", "utf-8", "latin1", "mac_roman"]
CSV_KWARGS_TRIES = [
    # fast path
    dict(engine="c", on_bad_lines="skip", quoting=csv.QUOTE_MINIMAL),
    # flexible path
    dict(engine="python", on_bad_lines="skip"),
]
DELIMS_TO_TRY = [None, ",", "\t", ";", "|", "^"]  # None => sniff
HEADER_CANDIDATES = ["infer", None, 0]  # try to detect header / headerless

# ==================================================

def list_files(root, recursive=True):
    exts = (".csv", ".tsv", ".txt", ".xlsx", ".xls")
    if not recursive:
        for fname in sorted(os.listdir(root)):
            full = os.path.join(root, fname)
            if os.path.isfile(full) and fname.lower().endswith(exts):
                yield full
    else:
        for dirpath, _, files in os.walk(root):
            for fname in sorted(files):
                if fname.lower().endswith(exts):
                    yield os.path.join(dirpath, fname)

def sniff_delimiter(sample_bytes):
    try:
        sample = sample_bytes.decode("utf-8", errors="ignore")
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|^")
        return dialect.delimiter
    except Exception:
        return None

def read_csv_safely(fullpath):
    last_err = None
    # read small sample for delimiter sniffing
    try:
        with open(fullpath, "rb") as fh:
            sample_bytes = fh.read(8192)
        sniffed = sniff_delimiter(sample_bytes)  # may be None
        delims = [sniffed] + [d for d in DELIMS_TO_TRY if d != sniffed]
    except Exception:
        delims = DELIMS_TO_TRY

    for enc in ENCODINGS:
        for header_opt in HEADER_CANDIDATES:
            for csv_kwargs in CSV_KWARGS_TRIES:
                for delim in delims:
                    try:
                        df = pd.read_csv(
                            fullpath,
                            encoding=enc,
                            nrows=SAMPLE_ROWS,
                            header=None if header_opt is None else 0 if header_opt == 0 else "infer",
                            sep=delim,
                            **csv_kwargs
                        )
                        # If header=None, make columns generic strings to avoid ints later
                        df.columns = [str(c) for c in df.columns]
                        return df
                    except Exception as e:
                        last_err = e
                        continue
    raise last_err

def read_excel_safely(fullpath):
    # returns list of (sheet_name, df) — possibly multiple sheets
    try:
        xls = pd.ExcelFile(fullpath)
        sheets = []
        for sheet in xls.sheet_names if TRY_SHEETS else [xls.sheet_names[0]]:
            try:
                df = pd.read_excel(xls, sheet_name=sheet, nrows=SAMPLE_ROWS)
                df.columns = [str(c) for c in df.columns]
                sheets.append((sheet, df))
            except Exception as e:
                sheets.append((sheet, None))
        return sheets
    except Exception as e:
        # try single read (older engines)
        try:
            df = pd.read_excel(fullpath, nrows=SAMPLE_ROWS)
            df.columns = [str(c) for c in df.columns]
            return [("Sheet1", df)]
        except Exception as ee:
            raise ee

def read_table(fullpath):
    lower = fullpath.lower()
    if lower.endswith(".csv") or lower.endswith(".tsv") or (lower.endswith(".txt") and TREAT_TXT_AS_CSV):
        df = read_csv_safely(fullpath)
        return [("CSV", df)]
    elif lower.endswith(".xlsx") or lower.endswith(".xls"):
        return read_excel_safely(fullpath)
    else:
        return []

# ---------- Classification helpers ----------

def is_numeric_series(s: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(s)

def is_categorical_series(s: pd.Series) -> bool:
    return pd.api.types.is_object_dtype(s) or pd.api.types.is_categorical_dtype(s)

def detect_wide_by_col_patterns(cols):
    """
    Find base names with time/period suffixes (years, quarters, months, day numbers).
    """
    pattern = re.compile(
        r"^(?P<base>.+?)[_\-]?(?P<suf>("
        r"\d{2,4}"               # e.g., 19, 2021
        r"|Q[1-4]"               # Q1..Q4
        r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"|(?:0?[1-9]|[12][0-9]|3[01])"  # day-of-month 1..31
        r"))$",
        re.IGNORECASE
    )
    groups = {}
    for c in cols:
        m = pattern.match(str(c))
        if m:
            base = m.group("base").strip()
            groups.setdefault(base, []).append(c)

    if not groups:
        return False, None, 0, {}

    max_group = 0
    max_group_name = None
    for base, members in groups.items():
        if len(members) > max_group:
            max_group = len(members)
            max_group_name = base

    strong = max_group >= MIN_WIDE_SUFFIX_GROUP
    return strong, max_group_name, max_group, groups

def detect_wide_by_structure(df: pd.DataFrame):
    """
    Additional 'wide' cues:
      - Many numeric columns (>=60%).
      - Very few rows relative to columns (cols/rows >= 0.5 AND cols >= 20).
    """
    nrows, ncols = df.shape
    if ncols == 0:
        return False, None

    numeric_cols = sum(1 for c in df.columns if is_numeric_series(df[c]))
    numeric_ratio = numeric_cols / max(ncols, 1)

    reasons = []
    wide_votes = 0

    if numeric_ratio >= 0.6 and ncols >= 8:
        wide_votes += 1
        reasons.append(f"many numeric columns ({numeric_cols}/{ncols} ~ {numeric_ratio:.0%})")

    if ncols >= 20 and (nrows > 0) and (ncols / nrows >= 0.5):
        wide_votes += 1
        reasons.append(f"more columns relative to rows (cols/rows={ncols}/{nrows})")

    return (wide_votes >= 1), "; ".join(reasons) if reasons else None

def detect_tall_schema(df: pd.DataFrame):
    """
    Tall cues:
      - A single (or very few) numeric 'value' column(s).
      - A 'variable/metric/measure' categorical column.
      - Repeated IDs → average rows per ID >= 2.
      - Many rows relative to columns.
    """
    nrows, ncols = df.shape
    if ncols == 0:
        return False, None

    cols_lower = [str(c).lower() for c in df.columns]
    numeric_cols = [c for c in df.columns if is_numeric_series(df[c])]
    non_numeric_cols = [c for c in df.columns if not is_numeric_series(df[c])]

    likely_value_names = {"value", "amount", "val", "measurement", "score", "reading"}
    likely_variable_names = {"variable", "metric", "measure", "attribute", "feature", "name", "type", "category"}

    has_named_value = any(c.lower() in likely_value_names for c in df.columns)
    has_named_variable = any(c.lower() in likely_variable_names for c in df.columns)

    single_numeric = len(numeric_cols) == 1
    few_numeric = len(numeric_cols) <= 2

    # ID-like columns: high uniqueness ratio
    id_candidates = []
    for c in non_numeric_cols:
        try:
            nunique = df[c].nunique(dropna=True)
            if nrows > 0 and nunique >= 0.5 * nrows:
                id_candidates.append(c)
        except Exception:
            continue

    long_repetition = False
    repeated_id_name = None
    for cid in id_candidates:
        try:
            grp = df.groupby(cid, dropna=False).size()
            avg_per_id = grp.mean() if len(grp) else 0
            if avg_per_id >= 2.0:
                long_repetition = True
                repeated_id_name = cid
                break
        except Exception:
            continue

    reasons = []
    tall_votes = 0

    if has_named_value:
        tall_votes += 1
        reasons.append("found 'value-like' column name")
    if has_named_variable:
        tall_votes += 1
        reasons.append("found 'variable/metric' column name")
    if single_numeric or few_numeric:
        tall_votes += 1
        reasons.append("one/few numeric measure columns")
    if long_repetition:
        tall_votes += 1
        reasons.append(f"repeated IDs: avg ≥ 2 rows per '{repeated_id_name}'")
    if nrows > ncols * 5 and nrows >= 100:  # many more rows than columns
        tall_votes += 1
        reasons.append(f"rows ≫ cols ({nrows} ≫ {ncols})")

    return (tall_votes >= 2), "; ".join(reasons) if reasons else None

def fallback_shape(df: pd.DataFrame):
    nrows, ncols = df.shape
    if nrows > ncols:
        return "tall", f"fallback: rows>cols ({nrows}>{ncols})"
    else:
        return "wide", f"fallback: cols>=rows ({ncols}>={nrows})"

def classify(df: pd.DataFrame):
    nrows, ncols = df.shape

    # 0) Handle degenerate cases
    if nrows == 0 or ncols == 0:
        return "unknown", "empty/degenerate data frame"

    # 1) Pattern-based wide detection
    is_wide_pattern, base_name, group_size, groups = detect_wide_by_col_patterns(df.columns)
    if is_wide_pattern:
        reason = f"wide: {group_size} columns share base '{base_name}' with time/index suffixes"
        return "wide", reason

    # 2) Structural wide cues
    wide_struct, wide_struct_reason = detect_wide_by_structure(df)
    if wide_struct:
        return "wide", f"wide: {wide_struct_reason}"

    # 3) Schema-based tall detection
    tall_flag, tall_reason = detect_tall_schema(df)
    if tall_flag:
        return "tall", f"tall: {tall_reason}"

    # 4) Fallback
    return fallback_shape(df)

def main():
    records = []
    errors = []

    for full in list_files(ROOT_PATH, RECURSIVE):
        base = os.path.basename(full)
        try:
            tables = read_table(full)  # list of (label, df) — label is "CSV" or sheet name
            if not tables:
                errors.append((full, "unrecognized or unreadable"))
                continue

            for label, df in tables:
                tag = base if label == "CSV" else f"{base}::{label}"
                if df is None or df.empty:
                    records.append({
                        "file": tag,
                        "rows": 0,
                        "cols": 0,
                        "classification": "unknown",
                        "reason": "empty or unreadable sheet"
                    })
                    continue

                # Basic cleanup cue: if first row looks like headers accidentally in data, try to fix?
                # (We won't mutate here; just classify as-is for safety.)

                cls, reason = classify(df)
                r, c = int(df.shape[0]), int(df.shape[1])
                records.append({
                    "file": tag,
                    "rows": r,
                    "cols": c,
                    "classification": cls,
                    "reason": reason
                })

        except Exception as e:
            errors.append((full, str(e)))

    # Print summary
    tall = [r for r in records if r["classification"] == "tall"]
    wide = [r for r in records if r["classification"] == "wide"]
    unknown = [r for r in records if r["classification"] == "unknown"]

    print(f"Tall files: {len(tall)}")
    for r in tall:
        print(f"  - {r['file']} (rows={r['rows']}, cols={r['cols']}) :: {r['reason']}")

    print(f"\nWide files: {len(wide)}")
    for r in wide:
        print(f"  - {r['file']} (rows={r['rows']}, cols={r['cols']}) :: {r['reason']}")

    if unknown:
        print(f"\nUnknown files: {len(unknown)}")
        for r in unknown:
            print(f"  - {r['file']} (rows={r['rows']}, cols={r['cols']}) :: {r['reason']}")

    if errors:
        print("\n Files with read errors:")
        for fn, msg in errors:
            print(f"  - {fn} :: {msg}")

    # Export
    if EXPORT_SUMMARY:
        out = pd.DataFrame(records)
        out.sort_values(["classification", "file"], inplace=True)
        out_path = os.path.join(ROOT_PATH, SUMMARY_NAME)
        out.to_csv(out_path, index=False)
        print(f"\nSummary saved to: {out_path}")

if __name__ == "__main__":
    main()
