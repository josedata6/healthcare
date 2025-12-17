"""
ETL for WA hospital price transparency CSVs -> tall Postgres table hp.charge_long

Usage:
  python hp_long_loader.py --data-dir "/Users/jdd48774/Downloads/hospital-data" \
      --pg "postgresql://jdd48774:bana@localhost:5437/hospital" \
      --schema hp --table charge_long

Dependencies:
  pip install pandas psycopg[binary] python-dateutil
"""

import argparse
import os
import re
import sys
from io import StringIO
from glob import glob

import pandas as pd
import psycopg

# ----------------- Default Config -----------------
DEFAULT_DATA_DIR = "/Users/jdd48774/Downloads/hospital-data"
DEFAULT_PG_DSN   = "postgresql://jdd48774:bana@localhost:5437/hospital"
DEFAULT_SCHEMA   = "hp"
DEFAULT_TABLE    = "charge_long"
# --------------------------------------------------

ENCODING_TRY = ["utf-8", "utf-8-sig", "cp1252", "latin-1"]
CANDIDATE_DELIMS = [",", "\t", ";"]
NORM_RE = re.compile(r"[^a-z0-9|]+")  # keep letters, digits, and the pipe

def norm_key(s: str) -> str:
    return NORM_RE.sub("_", str(s).strip().lower()).strip("_")

# synonyms for meta fields (keys are canonical output names)
META_SYNONYMS = {
    "hospital_name": {"hospital_name","facility_name","provider_name","hospital","facility"},
    "hospital_location": {"hospital_location","location","address_city_state_zip","city_state_zip","city_state_zip_code"},
    "hospital_address": {"hospital_address","address","street","street_address","address_line_1","address_1"},
    "last_updated_on": {"last_updated_on","last_updated","updated_on","update_date","date"},
    "version": {"version","schema_version","file_version"},
    "license_number|ca": {
        "license_number|ca","license_number","facility_license_number","hospital_license_number",
        "license_no","state_license_number","license","license_number|wa"
    },
}

def read_csv_with_fallback(path, **kwargs):
    last_err = None
    for enc in ENCODING_TRY:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs), enc
        except UnicodeDecodeError as e:
            last_err = e
            continue
    raise last_err

SENTINEL_NULLS = {
    "na","n/a","none","null","nan","not disclosed","not_disclosed","not available","n.a."
}
SENTINEL_BIG = {"999999999", "999999999.0", "999999999.00"}

def clean_amount_like(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    sl = s.lower()
    if sl in SENTINEL_NULLS:
        return None
    s2 = s.replace("$","").replace(",","").strip()
    if s2 in SENTINEL_BIG:
        return None
    return s  # keep original string (storing as text)

def normalize_str(x):
    if isinstance(x, str):
        return x.replace("\xa0", " ").strip()
    return x

def sniff_delimiter(path, default=",", sample_lines=6):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = [next(f, "") for _ in range(sample_lines)]
    if not head:
        return default
    header_raw = head[min(2, len(head)-1)]
    contains_scg = "standard_charge|gross" in header_raw
    best = default
    best_score = (-1, -10**9)  # (median_cols, -variance)
    for d in CANDIDATE_DELIMS:
        if d == "|" and contains_scg:
            continue
        counts = [len(line.rstrip("\n").split(d)) for line in head if line]
        if not counts:
            continue
        counts_sorted = sorted(counts)
        median_cols = counts_sorted[len(counts_sorted)//2]
        variance = max(counts) - min(counts)
        score = (median_cols, -variance)
        if score > best_score:
            best_score = score
            best = d
    return best

def find_true_header_row(path, delim, max_scan=10):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i in range(max_scan):
            line = f.readline()
            if not line:
                break
            cols = [c.strip().lower() for c in line.rstrip("\n").split(delim)]
            if "description" in cols and sum(1 for c in cols if c.startswith("code")) >= 2:
                return i
    return 2  # fallback: row 3 is header

def iter_chunks_with_encoding_fallback(path, hdr_idx, delim, chunksize):
    for enc in ENCODING_TRY:
        try:
            it = pd.read_csv(
                path, skiprows=hdr_idx, header=0, dtype=str, sep=delim, engine="c",
                chunksize=chunksize, on_bad_lines="skip", encoding=enc
            )
            for ch in it:
                yield ch
            return
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, "All encodings failed")

# ---------- Config for wide detection ----------
WIDE_BASE_COLS_ORDER = [
    "description",
    "code",
    "code|1", "code|1|type",
    "code|2", "code|2|type",
    "code|3", "code|3|type",
    "code|4", "code|4|type",
    "code|5", "code|5|type",
    "code|6", "code|6|type",
    "modifiers",
    "setting",
    "drug_unit_of_measurement",
    "drug_type_of_measurement",
    "standard_charge|gross",
    "standard_charge|discounted_cash",
    "additional_generic_notes",
    "billing_class",
    "activity_type",
]

NEGOTIATED_METRICS = {"negotiated_dollar", "negotiated_percentage", "negotiated_algorithm"}
IGNORE_STANDARD_CHARGE_SUFFIXES = {"min", "max"}
CHUNKSIZE = 75_000

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=DEFAULT_DATA_DIR, help="Folder containing *.csv files")
    ap.add_argument("--pg", default=DEFAULT_PG_DSN, help="Postgres connection string")
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--delimiter", default=",", help="CSV delimiter (default ,)")
    ap.add_argument("--encoding", default="utf-8")
    return ap.parse_args()

def clean_name(s):
    if s is None:
        return None
    return re.sub(r"\s+", " ", str(s)).strip()

# ---------- Yield tall rows (your existing logic, unchanged) ----------
def yield_tall_rows(meta, chunk):
    orig_cols = list(chunk.columns)
    chunk.columns = [str(c).strip() for c in orig_cols]
    cols_lower = {c.lower(): c for c in chunk.columns}

    def get_ci(row, *cands):
        for cand in cands:
            c = cols_lower.get(cand.lower())
            if c is not None:
                v = row.get(c)
                if pd.isna(v) or (isinstance(v, str) and v.strip() == ""):
                    continue
                return v
        return None

    def get_ci_amount(row, *cands):
        v = get_ci(row, *cands)
        return clean_amount_like(v)

    gross_col = "standard_charge|gross" if "standard_charge|gross" in chunk.columns else None
    disc_col  = "standard_charge|discounted_cash" if "standard_charge|discounted_cash" in chunk.columns else None

    ROWWISE_SYNS = {
        "payer_name": ["payer_name", "payer", "insurance", "insurance_name", "payername"],
        "plan_name":  ["plan_name", "plan", "plan_type", "insurance_type", "network", "coverage_type"],
        "standard_charge": ["standard_charge", "standard charge", "standardcharge", "charge", "price"],
        "negotiated_dollar": ["negotiated_dollar", "negotiated dollar", "negotiated_dollar_amount", "negotiated rate", "negotiated amount"],
        "negotiated_percentage": ["negotiated_percentage", "negotiated percent", "negotiated_rate_pct", "negotiated rate %"],
        "negotiated_algorithm": ["negotiated_algorithm", "algorithm", "negotiated algo", "calc algorithm"],
        "estimated_amount": ["estimated_amount", "estimate", "est_amount", "estimated amount"],
        "methodology": ["methodology", "calc_method", "calculation_method"],
        "additional_notes": ["additional_payer_notes", "payer_notes", "additional notes", "notes"],
        "gross": ["standard_charge|gross", "gross charge", "gross"],
        "discounted_cash": ["standard_charge|discounted_cash", "cash price", "discounted cash", "cash_discount"],
    }

    def any_col_present(syn_list):
        return any(s.lower() in cols_lower for s in syn_list)

    looks_rowwise = any_col_present(ROWWISE_SYNS["payer_name"]) and any_col_present(ROWWISE_SYNS["plan_name"])

    lower_cols = {c.lower(): c for c in chunk.columns}
    prefixes_lc = ("standard_charge|", "estimated_amount|", "additional_payer_notes|", "methodology|")
    payer_cols = []
    for lc, orig in lower_cols.items():
        if any(lc.startswith(p) for p in prefixes_lc):
            if lc in ("standard_charge|gross", "standard_charge|discounted_cash"):
                continue
            payer_cols.append(orig)

    for _, row in chunk.iterrows():
        base = {
            "file_name": meta.get("file_name"),
            "hospital_name": meta.get("hospital_name"),
            "hospital_location": meta.get("hospital_location"),
            "hospital_address": meta.get("hospital_address"),
            "license_number|CA": meta.get("license_number|CA"),
            "last_updated_on": meta.get("last_updated_on"),
            "version": meta.get("version"),

            "description": row.get("description", None),
            "drug_unit_of_measurement": row.get("drug_unit_of_measurement", None),
            "drug_type_of_measurement": row.get("drug_type_of_measurement", None),
            "code": row.get("code", None),
            "code|1": row.get("code|1", None),
            "code|1|type": row.get("code|1|type", None),
            "code|2": row.get("code|2", None),
            "code|2|type": row.get("code|2|type", None),
            "code|3": row.get("code|3", None),
            "code|3|type": row.get("code|3|type", None),
            "code|4": row.get("code|4", None),
            "code|4|type": row.get("code|4|type", None),
            "code|5": row.get("code|5", None),
            "code|5|type": row.get("code|5|type", None),
            "code|6": row.get("code|6", None),
            "code|6|type": row.get("code|6|type", None),
            "modifiers": row.get("modifiers", None),
            "setting": row.get("setting", None),
            "billing_class": row.get("billing_class", None),

            "estimated_amount": None,
            "activity_type": row.get("activity_type", None),

            "gross": clean_amount_like(row.get(gross_col)) if gross_col else get_ci_amount(row, *ROWWISE_SYNS["gross"]),
            "standard_charge": None,
            "discounted_cash": clean_amount_like(row.get(disc_col)) if disc_col else get_ci_amount(row, *ROWWISE_SYNS["discounted_cash"]),

            "negotiated_dollar": None,
            "negotiated_percentage": None,
            "negotiated_algorithm": None,
            "methodology": None,
            "additional_notes": row.get("additional_generic_notes", None),
        }

        if looks_rowwise and not payer_cols:
            payer = get_ci(row, *ROWWISE_SYNS["payer_name"])
            plan  = get_ci(row, *ROWWISE_SYNS["plan_name"])
            out = dict(base)
            out.update({
                "payer_name": payer,
                "plan_name": plan,
                "standard_charge": get_ci_amount(row, *ROWWISE_SYNS["standard_charge"]),
                "negotiated_dollar": get_ci_amount(row, *ROWWISE_SYNS["negotiated_dollar"]),
                "negotiated_percentage": get_ci_amount(row, *ROWWISE_SYNS["negotiated_percentage"]),
                "negotiated_algorithm": get_ci(row, *ROWWISE_SYNS["negotiated_algorithm"]),
                "estimated_amount": get_ci_amount(row, *ROWWISE_SYNS["estimated_amount"]),
                "methodology": get_ci(row, *ROWWISE_SYNS["methodology"]),
                "additional_notes": get_ci(row, *ROWWISE_SYNS["additional_notes"]) or out["additional_notes"],
            })
            yield out
            continue

        per = {}
        for col in payer_cols:
            parts = col.split("|")
            key0 = parts[0].lower()

            if key0 == "standard_charge":
                if len(parts) >= 4:
                    payer = clean_name(parts[1])
                    plan  = clean_name(parts[2])
                    metric = parts[3].strip().lower()
                    if metric in IGNORE_STANDARD_CHARGE_SUFFIXES:
                        continue
                    if (payer, plan) not in per:
                        per[(payer, plan)] = {}
                    val = clean_amount_like(row.get(col))
                    if val is None:
                        continue
                    if metric in NEGOTIATED_METRICS or metric == "standard_charge":
                        per[(payer, plan)][metric] = val

            elif key0 == "estimated_amount":
                if len(parts) >= 2:
                    payer = clean_name(parts[1])
                    plan  = clean_name(parts[2]) if len(parts) >= 3 else None
                    if (payer, plan) not in per:
                        per[(payer, plan)] = {}
                    val = clean_amount_like(row.get(col))
                    if val is not None:
                        per[(payer, plan)]["estimated_amount"] = val

            elif key0 == "methodology":
                if len(parts) >= 2:
                    payer = clean_name(parts[1])
                    plan  = clean_name(parts[2]) if len(parts) >= 3 else None
                    if (payer, plan) not in per:
                        per[(payer, plan)] = {}
                    val = row.get(col)
                    if pd.notna(val) and str(val).strip() != "":
                        per[(payer, plan)]["methodology"] = val

            elif key0 == "additional_payer_notes":
                if len(parts) >= 2:
                    payer = clean_name(parts[1])
                    plan  = clean_name(parts[2]) if len(parts) >= 3 else None
                    if (payer, plan) not in per:
                        per[(payer, plan)] = {}
                    val = row.get(col)
                    if pd.notna(val) and str(val).strip() != "":
                        per[(payer, plan)]["additional_notes"] = val

        if not per:
            out = dict(base)
            out.update({"payer_name": None, "plan_name": None})
            yield out
        else:
            for (payer, plan), metrics in per.items():
                out = dict(base)
                out.update({
                    "payer_name": payer,
                    "plan_name": plan,
                    "standard_charge": clean_amount_like(metrics.get("standard_charge")),
                    "negotiated_dollar": clean_amount_like(metrics.get("negotiated_dollar")),
                    "negotiated_percentage": clean_amount_like(metrics.get("negotiated_percentage")),
                    "negotiated_algorithm": metrics.get("negotiated_algorithm"),
                    "estimated_amount": clean_amount_like(metrics.get("estimated_amount", out["estimated_amount"])),
                    "methodology": metrics.get("methodology", out["methodology"]),
                    "additional_notes": metrics.get("additional_notes", out["additional_notes"]),
                })
                yield out

# ---------- DB helpers ----------
TARGET_COLS = [
    "file_name",
    "hospital_name", "hospital_location", "hospital_address", "license_number|CA",
    "last_updated_on", "version",
    "description", "drug_unit_of_measurement", "drug_type_of_measurement",
    "code", "code|1", "code|1|type", "code|2", "code|2|type", "code|3", "code|3|type",
    "code|4", "code|4|type", "code|5", "code|5|type", "code|6", "code|6|type",
    "modifiers", "setting", "billing_class",
    "payer_name", "plan_name",
    "estimated_amount", "activity_type",
    "gross", "standard_charge", "discounted_cash",
    "negotiated_dollar", "negotiated_percentage", "negotiated_algorithm",
    "methodology", "additional_notes",
]

DDL = f"""
CREATE SCHEMA IF NOT EXISTS "{DEFAULT_SCHEMA}";

CREATE TABLE IF NOT EXISTS "{DEFAULT_SCHEMA}"."{DEFAULT_TABLE}" (
    "file_name" text,
    "hospital_name" text,
    "hospital_location" text,
    "hospital_address" text,
    "license_number|CA" text,
    "last_updated_on" text,
    "version" text,

    "description" text,
    "drug_unit_of_measurement" text,
    "drug_type_of_measurement" text,

    "code" text,
    "code|1" text, "code|1|type" text,
    "code|2" text, "code|2|type" text,
    "code|3" text, "code|3|type" text,
    "code|4" text, "code|4|type" text,
    "code|5" text, "code|5|type" text,
    "code|6" text, "code|6|type" text,

    "modifiers" text,
    "setting" text,
    "billing_class" text,

    "payer_name" text,
    "plan_name" text,

    "estimated_amount" text,
    "activity_type" text,

    "gross" text,
    "standard_charge" text,
    "discounted_cash" text,

    "negotiated_dollar" text,
    "negotiated_percentage" text,
    "negotiated_algorithm" text,

    "methodology" text,
    "additional_notes" text
);
"""

def ensure_table(conn, schema, table, required_cols):
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        cur.execute(DDL)  # safe: IF NOT EXISTS

        # fetch existing columns
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table))
        have = {r[0] for r in cur.fetchall()}
        missing = [c for c in required_cols if c not in have]
        if missing:
            # add any missing columns as text
            for c in missing:
                cur.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{c}" text;')
            print(f"  -> Added missing columns: {', '.join(missing)}")

def copy_rows(conn, schema, table, rows, columns):
    import csv
    from io import StringIO

    def qident(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    buf = StringIO()
    writer = csv.writer(
        buf, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL,
        lineterminator="\n", doublequote=True, escapechar=None
    )
    NULL_TOKEN = r"\N"
    for r in rows:
        writer.writerow([NULL_TOKEN if r.get(c) is None else str(r.get(c)) for c in columns])

    payload = buf.getvalue()
    full = f'{qident(schema)}.{qident(table)}'
    cols_sql = ", ".join(qident(c) for c in columns)
    sql = (
        f"COPY {full} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER false, "
        f"DELIMITER ',', QUOTE '\"', NULL '\\N')"
    )
    with conn.cursor() as cur:
        with cur.copy(sql) as cp:
            cp.write(payload)

# ---------- Main ----------
def main():
    args = parse_args()

    files = sorted(glob(os.path.join(args.data_dir, "*.csv")))
    if not files:
        print("No CSV files found.", file=sys.stderr)
        sys.exit(1)

    with psycopg.connect(args.pg, autocommit=True) as conn:
        # Make sure table exists & columns match
        ensure_table(conn, args.schema, args.table, TARGET_COLS)

        for path in files:
            fname = os.path.basename(path)
            print(f"Processing: {fname}")
            try:
                delim = args.delimiter
                if delim in (r"\t", "\\t", "`t", "TAB", "tab"):
                    delim = "\t"
                explicit_delim = any(flag in " ".join(sys.argv) for flag in ["--delimiter", "-delimiter"])
                if not explicit_delim:
                    delim = sniff_delimiter(path, default=",")

                hdr_idx = find_true_header_row(path, delim)
                print(f"  -> using delimiter {repr(delim)}; header at line index {hdr_idx}")

                meta_df, enc_used = read_csv_with_fallback(
                    path, nrows=hdr_idx, header=None, dtype=str, sep=delim, engine="c"
                )
                print(f"  -> using encoding {enc_used} for meta")

                meta_map = {"file_name": fname}
                if meta_df is not None and meta_df.shape[0] >= 2:
                    raw_keys = [norm_key(x) for x in meta_df.iloc[0].tolist()]
                    raw_vals = [normalize_str(x) if pd.notna(x) else None for x in meta_df.iloc[1].tolist()]
                    kv = dict(zip(raw_keys, raw_vals))
                    for out_key, syns in META_SYNONYMS.items():
                        for candidate in syns:
                            if candidate in kv and kv[candidate]:
                                meta_map[out_key if out_key != "license_number|ca" else "license_number|CA"] = kv[candidate]
                                break
                    if meta_map.get("license_number|CA") is None:
                        for k, v in kv.items():
                            if "license" in k and "number" in k and v:
                                meta_map["license_number|CA"] = v
                                break

                for k in list(meta_map.keys()):
                    meta_map[k] = normalize_str(meta_map[k])

                print("  -> streaming data …")
                stream = iter_chunks_with_encoding_fallback(path, hdr_idx, delim, CHUNKSIZE)

                batch = []
                batch_rows = 0
                total_rows = 0

                for chunk in stream:
                    optional = ("billing_class", "activity_type", "additional_generic_notes")
                    missing = [c for c in optional if c not in chunk.columns]
                    if missing:
                        chunk = chunk.reindex(columns=[*chunk.columns, *missing])
                    chunk = chunk.where(pd.notnull(chunk), None)
                    for col in chunk.select_dtypes(include="object").columns:
                        chunk[col] = chunk[col].map(normalize_str)

                    for tall in yield_tall_rows(meta_map, chunk):
                        batch.append(tall)
                        batch_rows += 1
                        total_rows += 1
                        if batch_rows >= 150_000:
                            copy_rows(conn, args.schema, args.table, batch, TARGET_COLS)
                            print(f"  -> copied {batch_rows} rows")
                            batch.clear()
                            batch_rows = 0

                if batch_rows:
                    copy_rows(conn, args.schema, args.table, batch, TARGET_COLS)
                    print(f"  -> copied {batch_rows} rows (final for file)")

                print(f"  -> inserted ~{total_rows} rows for {fname}")
                with conn.cursor() as cur:
                    cur.execute(f'SELECT COUNT(*) FROM "{args.schema}"."{args.table}"')
                    print(f"  -> total rows now in {args.schema}.{args.table}: {cur.fetchone()[0]}")

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                print(f"  !! Skipped {fname} due to: {e}")
                continue

    print("DONE.")

if __name__ == "__main__":
    main()
