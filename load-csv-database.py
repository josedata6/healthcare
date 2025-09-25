# load_prices.py
import os, glob, io, gzip, re, json
import pandas as pd
from sqlalchemy import create_engine

# ---------- CONFIG ----------
DB_URL  = "postgresql+psycopg2://jdd48774:bana@127.0.0.1:5435/project650"
DATA_DIR = "/Users/jdd48774/Downloads/raw_data5kbs"  # folder with CSV/CSV.GZ
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
    c = c.replace("-", "_")
    c = re.sub(r"\s*\|\s*", "|", c)   # " a | b " → "a|b"
    c = re.sub(r"\s+", " ", c)
    return c

def is_charge_col(col: str) -> bool:
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
    b = col.split("__", 1)[0]  # strip any __N suffix
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

def _make_unique(names):
    seen = {}
    out = []
    for n in names:
        if n not in seen:
            seen[n] = 0
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}__{seen[n]}")
    return out

def _base_name(name):
    return name.split("__", 1)[0]

# ---------- filename → hospital name ----------
_BOILER_PATTERNS = [
    r"standard[_-]?charges?", r"machine[_-]?readable",
    r"(price|prices?)", r"chargemaster", r"cdm",
    r"inpatient", r"outpatient", r"shoppable"
]
def guess_hospital_name_from_filename(path: str) -> str:
    base = os.path.basename(path)
    base = re.sub(r"\.csv(\.gz)?$", "", base, flags=re.IGNORECASE)
    s = base.replace("_", " ").replace("-", " ")
    s = re.sub(r"^[0-9]{2}-[0-9]{7}\s+", "", s)
    s = re.sub(r"^[0-9]{8}\s+", "", s)
    s = re.sub(r"^[0-9]{9}\s+", "", s)
    s = re.sub(r"^[0-9]+\s+", "", s)
    for pat in _BOILER_PATTERNS:
        s = re.sub(rf"\b{pat}\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s.title() or base.title()

# ---------- Melt & normalize ----------
def melt_and_normalize(df: pd.DataFrame, hospital_name: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [normalize_header(c) for c in df.columns]
    if len(set(df.columns)) != len(df.columns):
        df.columns = _make_unique(df.columns)
        print("   ↳ detected duplicate headers; made them unique.")
    cols = list(df.columns)

    def pick_first(*candidates):
        cand_set = set(candidates)
        for name in cols:
            if _base_name(name) in cand_set:
                return name
        return None

    # Pick ID columns (keep plan_name separate from payer_name)
    col_code        = pick_first("code|1", "code")
    col_code_type   = pick_first("code|1|type", "code_type")
    col_code2       = pick_first("code|2")
    col_code2_type  = pick_first("code|2|type")

    col_payer_name  = pick_first("payer_name")
    col_plan_name   = pick_first("plan_name")  # NEW: keep explicitly
    col_bclass      = pick_first("billing_class")
    col_setting     = pick_first("setting")
    col_currency    = pick_first("currency")
    col_eff_date    = pick_first("effective_date", "start_date")
    col_exp_date    = pick_first("expires_on", "end_date")
    col_desc_hint   = pick_first("description")
    col_modifiers   = pick_first("modifiers")
    col_drug_uom    = pick_first("drug_unit_of_measurement")
    col_drug_type   = pick_first("drug_type_of_measurement")
    col_neg_algo    = pick_first("standard_charge|negotiated_algorithm", "negotiated_algorithm")
    col_method      = pick_first("standard_charge|methodology", "methodology")
    col_est_amt     = pick_first("estimated_amount")
    col_add_notes   = pick_first("additional_generic_notes")
    col_metadata    = pick_first("metadata")  # if your cleaner added it

    # Build working frame with all we want to carry through the melt
    use_cols = []
    for c in [
        col_code, col_code_type, col_code2, col_code2_type,
        col_payer_name, col_plan_name, col_bclass, col_setting,
        col_currency, col_eff_date, col_exp_date, col_desc_hint,
        col_modifiers, col_drug_uom, col_drug_type, col_neg_algo,
        col_method, col_est_amt, col_add_notes, col_metadata
    ]:
        if c and c not in use_cols:
            use_cols.append(c)

    charge_cols = [c for c in cols if is_charge_col(c)]
    if not charge_cols:
        print("No standard_charge-like columns detected; skipping this file.")
        return pd.DataFrame(columns=[
            "hospital_name","code","code_type","payer_name","plan_name","billing_class",
            "price_type","price_amount","currency","effective_date","expires_on",
            "description","modifiers","drug_unit_of_measurement","drug_type_of_measurement",
            "negotiated_algorithm","estimated_amount","methodology","additional_generic_notes",
            "metadata","code_2","code_2_type","notes"
        ])

    wdf = df[use_cols + charge_cols].copy()

    # Melt
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

    # Map price type
    long_df["price_type"] = long_df["charge_col"].map(col_to_price_type_from_name)
    long_df.drop(columns=["charge_col"], inplace=True)

    # Coalesce code/code_type across slots, keep slot 2 raw as separate columns
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

    long_df["code"]      = long_df.apply(lambda r: first_nonempty(r, code_cols), axis=1)
    long_df["code_type"] = long_df.apply(lambda r: first_nonempty(r, type_cols), axis=1)

    def norm_type(t):
        t = (t or "").strip().upper()
        if t in ("CPT®", "CPT-4", "CPT4"): return "CPT"
        if t in ("HCPCS", "HCPCS-CODE"):   return "HCPCS"
        if t in ("DRG", "MS-DRG", "MSDRG"):return "DRG"
        if t in ("ICD", "ICD10", "ICD-10"):return "ICD10"
        return t
    long_df["code_type"] = long_df["code_type"].map(norm_type)

    # Rename carried columns to canonical names for output
    rename_id = {}
    if col_payer_name: rename_id[col_payer_name] = "payer_name"
    if col_plan_name:  rename_id[col_plan_name]  = "plan_name"
    if col_bclass:     rename_id[col_bclass]     = "billing_class"
    if col_setting:    rename_id[col_setting]    = "setting"
    if col_currency:   rename_id[col_currency]   = "currency"
    if col_eff_date:   rename_id[col_eff_date]   = "effective_date"
    if col_exp_date:   rename_id[col_exp_date]   = "expires_on"
    if col_desc_hint:  rename_id[col_desc_hint]  = "description"
    if col_modifiers:  rename_id[col_modifiers]  = "modifiers"
    if col_drug_uom:   rename_id[col_drug_uom]   = "drug_unit_of_measurement"
    if col_drug_type:  rename_id[col_drug_type]  = "drug_type_of_measurement"
    if col_neg_algo:   rename_id[col_neg_algo]   = "negotiated_algorithm"
    if col_method:     rename_id[col_method]     = "methodology"
    if col_est_amt:    rename_id[col_est_amt]    = "estimated_amount"
    if col_add_notes:  rename_id[col_add_notes]  = "additional_generic_notes"
    if col_metadata:   rename_id[col_metadata]   = "metadata"
    if col_code2:      rename_id[col_code2]      = "code_2"
    if col_code2_type: rename_id[col_code2_type] = "code_2_type"

    long_df.rename(columns=rename_id, inplace=True)

    # Clean numeric price
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

    # Optional notes JSON (keep using it; explicit columns are also preserved)
    note_keys = ["description", "setting", "modifiers"]
    def pack_notes(row):
        d = {k: row.get(k, "") for k in note_keys}
        d = {k: v for k, v in d.items() if isinstance(v, str) and v.strip()}
        return json.dumps(d, ensure_ascii=False) if d else ""
    notes_series = long_df.apply(pack_notes, axis=1)

    def col_or_blank_local(df_, col):
        return df_[col].fillna("") if col in df_.columns else pd.Series([""] * len(df_), index=df_.index)

    out = pd.DataFrame({
        "hospital_name":             hospital_name,
        "code":                      col_or_blank_local(long_df, "code"),
        "code_type":                 col_or_blank_local(long_df, "code_type"),
        "code_2":                    col_or_blank_local(long_df, "code_2"),
        "code_2_type":               col_or_blank_local(long_df, "code_2_type"),
        "payer_name":                col_or_blank_local(long_df, "payer_name"),
        "plan_name":                 col_or_blank_local(long_df, "plan_name"),
        "billing_class":             col_or_blank_local(long_df, "billing_class"),
        "price_type":                col_or_blank_local(long_df, "price_type"),
        "price_amount":              col_or_blank_local(long_df, "price_amount"),
        "currency":                  col_or_blank_local(long_df, "currency"),
        "effective_date":            col_or_blank_local(long_df, "effective_date"),
        "expires_on":                col_or_blank_local(long_df, "expires_on"),
        "description":               col_or_blank_local(long_df, "description"),
        "modifiers":                 col_or_blank_local(long_df, "modifiers"),
        "drug_unit_of_measurement":  col_or_blank_local(long_df, "drug_unit_of_measurement"),
        "drug_type_of_measurement":  col_or_blank_local(long_df, "drug_type_of_measurement"),
        "negotiated_algorithm":      col_or_blank_local(long_df, "negotiated_algorithm"),
        "estimated_amount":          col_or_blank_local(long_df, "estimated_amount"),
        "methodology":               col_or_blank_local(long_df, "methodology"),
        "additional_generic_notes":  col_or_blank_local(long_df, "additional_generic_notes"),
        "metadata":                  col_or_blank_local(long_df, "metadata"),
        "notes":                     notes_series.fillna(""),
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
        "hospital_name","code","code_type","code_2","code_2_type",
        "price_type","price_amount","payer_name","plan_name","billing_class",
        "currency","effective_date","expires_on","description","modifiers",
        "drug_unit_of_measurement","drug_type_of_measurement","negotiated_algorithm",
        "estimated_amount","methodology","additional_generic_notes","metadata","notes"
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
        # idempotent per-file: remove prior rows for this file
        cur.execute("DELETE FROM public.hospital_prices WHERE source_file = %s;", (source_file,))
        # load fresh
        cur.copy_expert(f"""
            COPY public.hospital_prices
            ({",".join(cols)},source_file)
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
        hosp = guess_hospital_name_from_filename(path)
        print(f"--> Processing {os.path.basename(path)}  |  inferred hospital: {hosp}")
        df = read_csv_any(path)
        tidy = melt_and_normalize(df, hospital_name=hosp)
        if tidy.empty:
            print("No price rows produced; skipping!!!!")
            continue
        copy_to_single_table(tidy, os.path.basename(path))

    print("All files loaded into public.hospital_prices.")

if __name__ == "__main__":
    main()
