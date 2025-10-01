# load_prices.py
import os, glob, io, gzip, re, json
import pandas as pd
from sqlalchemy import create_engine

# ---------- CONFIG ----------
DB_URL  = "postgresql+psycopg2://jdd48774:bana@127.0.0.1:5435/project650"
DATA_DIR = "/Users/jdd48774/Downloads/out-tall" ## path for files to load
ENGINE   = create_engine(DB_URL, future=True)

# Map charge suffix -> canonical price_type
PRICE_TYPE_MAP = {
    "gross": "chargemaster",
    "gross_charge": "chargemaster",
    "gross_charges": "chargemaster",
    "discounted_cash": "cash",
    "negotiated_dollar": "negotiated",
    "negotiated_percentage": "percentage",
    "min": "min",
    "max": "max",
}

# ---------- File reading ----------
def open_any(path: str):
    """Open text or gzipped CSV as text, letting pandas handle encoding."""
    return gzip.open(path, "rt", errors="replace") if path.endswith(".gz") else open(path, "r", errors="replace")

def _try_read(path: str, encoding: str, sep, engine: str):
    df = pd.read_csv(
        open_any(path),
        dtype=str,
        encoding=encoding,
        sep=sep,                 # None => auto (engine='python')
        engine=engine,           # 'python' parser is more forgiving
        quotechar='"',
        on_bad_lines="skip",
        skipinitialspace=True,
        keep_default_na=False,
    ).fillna("")
    return df

def read_csv_any(path: str) -> pd.DataFrame:
    """Robust CSV reader trying common encodings and delimiters."""
    for encoding in ("utf-8", "latin-1"):
        for sep in (None, ",", "\t", "|", ";"):
            try:
                df = _try_read(path, encoding=encoding, sep=sep, engine="python")
                print(f"   ✓ Parsed with encoding={encoding} sep={'auto' if sep is None else sep}")
                return df
            except Exception:
                continue
    raise RuntimeError(f"Unable to parse {os.path.basename(path)} with common encodings/delimiters.")

# ---------- Header helpers ----------
def normalize_header(col: str) -> str:
    c = str(col).strip().lower()
    c = c.replace("-", "_")             # dashes → underscores
    c = re.sub(r"\s*\|\s*", "|", c)     # " a | b " → "a|b"
    c = re.sub(r"\s+", " ", c)          # collapse spaces
    return c

def is_charge_col(col: str) -> bool:
    """
    Accept many real-world variants:
      - standard_charge|gross, standard charge|gross, standard_charge (no suffix)
      - standard_charge|discounted_cash|min|max
      - bare names: gross_charge, chargemaster, discounted_cash, negotiated_dollar, negotiated_percentage, min, max
    """
    c = normalize_header(col)
    if c in ("standard_charge", "standard charge"):
        return True
    if c.startswith("standard_charge|") or c.startswith("standard charge|"):
        return True
    bare = c.replace(" ", "_")
    return bare in {
        "gross", "gross_charge", "gross_charges", "chargemaster",
        "discounted_cash", "negotiated_dollar", "negotiated_percentage",
        "min", "max"
    }

def col_to_price_type_from_name(col: str) -> str:
    """Map a (possibly deduped) charge column name to canonical price_type."""
    b = col.split("__", 1)[0]  # strip any __N suffix
    # extract suffix after "standard_charge|"
    if "|" in b:
        suffix = b.split("|", 1)[1]
    else:
        bare = b.replace(" ", "_")
        if bare in ("gross", "gross_charge", "gross_charges", "chargemaster", "standard_charge"):
            suffix = "gross"
        elif bare in ("discounted_cash",):
            suffix = "discounted_cash"
        elif bare in ("negotiated_dollar",):
            suffix = "negotiated_dollar"
        elif bare in ("negotiated_percentage",):
            suffix = "negotiated_percentage"
        elif bare in ("min", "max"):
            suffix = bare
        else:
            suffix = "gross"
    return PRICE_TYPE_MAP.get(suffix, "chargemaster")

def col_or_blank(df, col):
    """Return df[col] if it exists, else a same-length Series of empty strings."""
    if col in df.columns:
        return df[col].fillna("")
    else:
        return pd.Series([""] * len(df), index=df.index)

def _make_unique(names):
    seen = {}
    out = []
    for n in names:
        if n not in seen:
            seen[n] = 0
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}__{seen[n]}")  # e.g., standard_charge|gross__1
    return out

def _base_name(name):
    # strip our dedupe suffix like "__1"
    return name.split("__", 1)[0]

# ---------- Melt & normalize ----------
def melt_and_normalize(df: pd.DataFrame, hospital_name: str) -> pd.DataFrame:
    # 0) normalize and dedupe headers
    df = df.copy()
    df.columns = [normalize_header(c) for c in df.columns]
    if len(set(df.columns)) != len(df.columns):
        df.columns = _make_unique(df.columns)
        print("   ↳ detected duplicate headers; made them unique.")
    cols = list(df.columns)

    # helper: first existing column whose base-name matches any candidate
    def pick_first(*candidates):
        cand_set = set(candidates)
        for name in cols:
            if _base_name(name) in cand_set:
                return name
        return None

    # choose id cols without renaming originals
    col_code        = pick_first("code|1", "code")
    col_code_type   = pick_first("code|1|type", "code_type")
    col_payer_name  = pick_first("payer_name", "plan_name")
    col_bclass      = pick_first("billing_class")
    col_setting     = pick_first("setting")
    col_currency    = pick_first("currency")
    col_eff_date    = pick_first("effective_date", "start_date")
    col_exp_date    = pick_first("expires_on", "end_date")
    col_desc_hint   = pick_first("description")

    # build working frame
    use_cols = []
    for c in [col_code, col_code_type, col_payer_name, col_bclass, col_setting,
              col_currency, col_eff_date, col_exp_date, col_desc_hint]:
        if c and c not in use_cols:
            use_cols.append(c)

    # detect charge columns by pattern
    charge_cols = [c for c in cols if is_charge_col(c)]
    if not charge_cols:
        print("   ⚠️  No standard_charge-like columns detected; skipping this file.")
        return pd.DataFrame(columns=[
            "hospital_name","code","code_type","payer_name","billing_class",
            "price_type","price_amount","currency","effective_date","expires_on","description","notes"
        ])

    wdf = df[use_cols + charge_cols].copy()

    # melt (all names are unique strings)
    id_vars = use_cols[:]
    value_vars = charge_cols[:]
    print(f"   → melting with {len(id_vars)} id cols, {len(value_vars)} charge cols")
    long_df = pd.melt(
        wdf,
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="charge_col",
        value_name="price_amount_raw"
    )

    # map charge column -> price_type
    long_df["price_type"] = long_df["charge_col"].map(col_to_price_type_from_name)
    long_df.drop(columns=["charge_col"], inplace=True)

    # coalesce code & code_type across slots 1..4
    present = lambda L: [c for c in L if c in long_df.columns]
    code_cols = present(["code|1", "code|2", "code|3", "code|4", "code"])
    type_cols = present(["code|1|type", "code|2|type", "code|3|type", "code|4|type", "code_type"])

    def first_nonempty(row, candidates):
        for c in candidates:
            v = row.get(c, "")
            if isinstance(v, str) and v.strip():
                return v.strip()
        for c in candidates:
            v = row.get(c, "")
            if pd.notna(v) and str(v).strip():
                return str(v).strip()
        return ""

    long_df["code_any"]      = long_df.apply(lambda r: first_nonempty(r, code_cols), axis=1)
    long_df["code_type_any"] = long_df.apply(lambda r: first_nonempty(r, type_cols), axis=1)

    def norm_type(t):
        t = (t or "").strip().upper()
        if t in ("CPT®", "CPT-4", "CPT4"): return "CPT"
        if t in ("HCPCS", "HCPCS-CODE"):   return "HCPCS"
        if t in ("DRG", "MS-DRG", "MSDRG"):return "DRG"
        if t in ("ICD", "ICD10", "ICD-10"):return "ICD10"
        return t

    long_df["code_type_any"] = long_df["code_type_any"].map(norm_type)

    # rename chosen id columns to canonical names
    rename_id = {}
    if col_payer_name: rename_id[col_payer_name] = "payer_name"
    if col_bclass:     rename_id[col_bclass]     = "billing_class"
    if col_setting:    rename_id[col_setting]    = "setting"
    if col_currency:   rename_id[col_currency]   = "currency"
    if col_eff_date:   rename_id[col_eff_date]   = "effective_date"
    if col_exp_date:   rename_id[col_exp_date]   = "expires_on"
    if col_desc_hint:  rename_id[col_desc_hint]  = "description"
    long_df.rename(columns=rename_id, inplace=True)

    # use coalesced code/type
    long_df["code"]      = long_df["code_any"]
    long_df["code_type"] = long_df["code_type_any"]
    long_df.drop(columns=["code_any","code_type_any"], inplace=True)

    # description fallback from aliases if needed
    if "description" not in long_df.columns:
        long_df["description"] = ""

    # clean numeric price
    def to_numeric(val, ptype):
        v = (val or "").strip()
        if v == "": return ""
        if ptype == "percentage":
            v = v.replace("%","")
        try:
            return str(float(v))
        except:
            return ""
    long_df["price_amount"] = [to_numeric(v, t) for v, t in zip(long_df["price_amount_raw"], long_df["price_type"])]
    long_df.drop(columns=["price_amount_raw"], inplace=True, errors="ignore")

    # notes JSON
    note_keys = ["description", "setting", "modifiers"]
    def pack_notes(row):
        d = {k: row.get(k, "") for k in note_keys}
        d = {k: v for k, v in d.items() if isinstance(v, str) and v.strip()}
        return json.dumps(d, ensure_ascii=False) if d else ""
    notes_series = long_df.apply(pack_notes, axis=1)

    # assemble canonical frame
    def col_or_blank_local(df_, col):
        return df_[col].fillna("") if col in df_.columns else pd.Series([""] * len(df_), index=df_.index)

    out = pd.DataFrame({
        "hospital_name":  hospital_name,
        "code":           col_or_blank_local(long_df, "code"),
        "code_type":      col_or_blank_local(long_df, "code_type"),
        "payer_name":     col_or_blank_local(long_df, "payer_name"),
        "plan_name":     col_or_blank_local(long_df, "payer_name"), 
        "billing_class":  col_or_blank_local(long_df, "billing_class"),
        "price_type":     col_or_blank_local(long_df, "price_type"),
        "price_amount":   col_or_blank_local(long_df, "price_amount"),
        "currency":       col_or_blank_local(long_df, "currency"),
        "effective_date": col_or_blank_local(long_df, "effective_date"),
        "expires_on":     col_or_blank_local(long_df, "expires_on"),
        "description":    col_or_blank_local(long_df, "description"),
        "notes":          notes_series.fillna("")
    })

    # keep usable rows
    for c in ["code","code_type","price_amount"]:
        out[c] = out[c].astype(str).str.strip()
    keep = (out["code"] != "") & (out["code_type"] != "") & (out["price_amount"] != "")
    dropped = len(out) - int(keep.sum())
    if dropped > 0:
        print(f"   ↳ dropped {dropped} rows missing code/code_type/price")
    return out[keep].reset_index(drop=True)

# ---------- COPY into single table ----------
def copy_to_single_table(df, source_file):
    cols = [
        "hospital_name","code","code_type","price_type","price_amount",
        "payer_name",  "plan_name", "billing_class","currency","effective_date","expires_on",
        "description","notes"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df = df.copy()
    df["source_file"] = source_file

    buf = io.StringIO()
    df[cols + ["source_file"]].to_csv(buf, index=False, header=False)
    buf.seek(0)

    conn = ENGINE.raw_connection()
    try:
        cur = conn.cursor()
        # Use NULL '' so empty strings become NULL (safer for DATE/NUMERIC/JSONB)
        cur.copy_expert("""
            COPY public.hospital_prices
            (hospital_name, code, code_type, price_type, price_amount,
            payer_name, plan_name, billing_class, currency, effective_date, expires_on,
            description, notes, source_file)
            FROM STDIN WITH (FORMAT CSV, NULL '')
        """, buf)

        conn.commit()
        cur.close()
    finally:
        conn.close()

# ---------- MAIN ----------
def main():
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")) + glob.glob(os.path.join(DATA_DIR, "*.csv.gz")))
    if not files:
        print("No files found in", DATA_DIR)
        return

    for path in files:
        hosp = input(f"Hospital name for {os.path.basename(path)}: ").strip()
        print(f"--> Processing {os.path.basename(path)} for hospital={hosp}")
        df = read_csv_any(path)
        tidy = melt_and_normalize(df, hospital_name=hosp)
        if tidy.empty:
            print("   ⚠️  No price rows produced; skipping.")
            continue
        copy_to_single_table(tidy, os.path.basename(path))

    print("✅ All files loaded into public.hospital_prices.")

if __name__ == "__main__":
    main()