#!/usr/bin/env python3
# wide_to_tall.py
# Transform hospital price CSVs from wide to tall format.

import os, re, io, glob, json, gzip
import pandas as pd

# ---------------- CONFIG ----------------
IN_DIR  = "/Users/jdd48774/Downloads/wide-to-tall"      # <-- set input folder
OUT_DIR = "/Users/jdd48774/Downloads/out-tall"     # <-- set output folder
OVERWRITE = True                  # overwrite existing tall files

# ------------ Helpers: IO --------------
def open_any(path: str):
    return gzip.open(path, "rt", errors="replace") if path.endswith(".gz") else open(path, "r", errors="replace")

def _try_read(path: str, encoding: str, sep, engine: str):
    return pd.read_csv(
        open_any(path),
        dtype=str,
        encoding=encoding,
        sep=sep,                 # None => auto (engine='python')
        engine=engine,           # 'python' parser is more forgiving
        quotechar='"',
        on_bad_lines="skip",
        skipinitialspace=True,
        keep_default_na=False
    ).fillna("")

def read_csv_any(path: str) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1"):
        for sep in (None, ",", "\t", "|", ";"):
            try:
                df = _try_read(path, enc, sep, "python")
                print(f"   ✓ Parsed {os.path.basename(path)} with encoding={enc} sep={'auto' if sep is None else sep}")
                return df
            except Exception:
                continue
    raise RuntimeError(f"Unable to parse {os.path.basename(path)} with common encodings/delimiters.")

# --------- Header normalization ---------
def normalize_header(col: str) -> str:
    c = str(col).strip().lower()
    c = c.replace("-", "_")              # dashes → underscores
    c = re.sub(r"\s*\|\s*", "|", c)      # " a | b " → "a|b"
    c = re.sub(r"\s+", " ", c)           # collapse spaces
    return c

def make_unique(cols):
    seen = {}
    out = []
    for n in cols:
        if n not in seen:
            seen[n] = 0
            out.append(n)
        else:
            seen[n] += 1
            out.append(f"{n}__{seen[n]}")
    return out

def base_name(name: str) -> str:
    return name.split("__", 1)[0]

# -------- hospital name from filename --------
SEP = r"[_\- ]"   # underscore, hyphen, space (hyphen escaped)
_BOILER = [
    rf"standard{SEP}?charges?", rf"machine{SEP}?readable",
    r"(?:price|prices?)", r"chargemaster", r"cdm",
    r"inpatient", r"outpatient", r"shoppable"
]

def guess_hospital_name_from_filename(path: str) -> str:
    base = os.path.basename(path)
    base = re.sub(r"\.csv(\.gz)?$", "", base, flags=re.IGNORECASE)
    s = base.replace("_", " ").replace("-", " ")
    s = re.sub(r"^[0-9]{2}-[0-9]{7}\s+", "", s)  # EIN like 91-0750229
    s = re.sub(r"^[0-9]{8}\s+", "", s)           # YYYYMMDD
    s = re.sub(r"^[0-9]{9}\s+", "", s)           # 9-digit id
    s = re.sub(r"^[0-9]+\s+", "", s)             # any leading number
    for pat in _BOILER:
        s = re.sub(rf"\b{pat}\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s.title() or base.title()

# ------------- Price mapping -------------
PRICE_TYPE_MAP = {
    "gross": "chargemaster",
    "gross_charge": "chargemaster",
    "gross_charges": "chargemaster",
    "discounted_cash": "cash",
    "cash": "cash",
    "negotiated_dollar": "negotiated",
    "negotiated_rate": "negotiated",
    "payer_specific": "negotiated",
    "contracted_rate": "negotiated",
    "allowed_amount": "negotiated",
    "percentage": "percentage",
    "negotiated_percentage": "percentage",
    "min": "min",
    "max": "max",
}

# robust detector for wide price columns (header → price_type, payer/plan if embedded)
def parse_price_header(colname: str):
    c = base_name(normalize_header(colname))  # strip __N
    ptype = None
    payer = None
    plan  = None

    # canonical "standard_charge|suffix"
    if c.startswith("standard_charge|") or c.startswith("standard charge|"):
        suffix = c.split("|", 1)[1]
        ptype = PRICE_TYPE_MAP.get(suffix, None)

    # bare standard_charge
    if ptype is None and c in ("standard_charge", "standard charge"):
        ptype = "chargemaster"

    # bare aliases (gross, cash, min/max, negotiated words)
    if ptype is None:
        b = c.replace(" ", "_")
        if b in PRICE_TYPE_MAP:
            ptype = PRICE_TYPE_MAP[b]

    # heuristic: negotiated with payer in header, e.g. "Aetna - Negotiated Rate"
    if ptype is None:
        if re.search(r"(?:\bnegotiated\b|\bpayer(?:[_ ]?specific)?\b|\ballowed\b|\bcontracted\b)", c):
            ptype = "negotiated"
            # try to extract leading payer/plan text before the negotiated keyword
            m = re.split(r"(?:\bnegotiated\b|\bpayer(?:[_ ]?specific)?\b|\ballowed\b|\bcontracted\b)", c, maxsplit=1)
            if m and m[0].strip():
                raw = m[0]
                # clean separators
                raw = re.sub(r"[_\-|]+", " ", raw)     # underscores/hyphens/pipes → spaces
                raw = re.sub(r"\s+", " ", raw).strip()
                if raw:
                    payer = raw.title()

    if ptype is None:
        return None, None, None
    return ptype, payer, plan

def detect_wide_price_columns(cols):
    out = []
    for c in cols:
        ptype, payer, plan = parse_price_header(c)
        if ptype:
            out.append((c, ptype, payer, plan))
    return out

# ---------- transform to tall ------------
DESC_ALIASES = [
    "description","service_description","procedure_description","item_description",
    "long_description","short_description","charge_description",
    "standard_charge_description","display_description","name","label"
]

def first_existing(cols, names):
    cand = set(names)
    for n in cols:
        if base_name(n) in cand:
            return n
    return None

def coalesce_first(row, candidates):
    for c in candidates:
        v = row.get(c, "")
        if isinstance(v, str) and v.strip():
            return v.strip()
    for c in candidates:
        v = row.get(c, "")
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
    return ""

def maybe_top_metadata(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    """
    If the first two rows look like a metadata banner (long text / not real headers),
    capture them into a JSON string and drop them. Otherwise return df, "".
    """
    if df.empty: return df, ""
    # Heuristic: if the *values* of first row contain a very long sentence,
    # or expected headers are NOT present but the first two rows have lots of long cells.
    head = list(df.columns)
    norm = [normalize_header(x) for x in head]
    has_any_expected = any(h.startswith("standard_charge") or h in ("code","code|1","code|1|type","payer_name","plan_name") for h in norm)

    if has_any_expected:
        return df, ""  # looks fine

    # build metadata dict from first two rows
    meta_rows = []
    for i in range(min(2, len(df))):
        row = df.iloc[i].to_dict()
        # keep only non-empty, short-ish keys
        row = {str(k): str(v) for k, v in row.items() if str(v).strip() != ""}
        if row:
            meta_rows.append(row)

    if not meta_rows:
        return df, ""

    meta_json = json.dumps({"header_rows": meta_rows}, ensure_ascii=False)
    df2 = df.iloc[2:].reset_index(drop=True) if len(df) > 2 else df.iloc[0:0]
    return df2, meta_json

def to_tall(df: pd.DataFrame, hospital_name: str) -> pd.DataFrame:
    # 1) normalize & unique headers
    df = df.copy()
    df.columns = [normalize_header(c) for c in df.columns]
    if len(set(df.columns)) != len(df.columns):
        df.columns = make_unique(df.columns)

    # 2) drop metadata top rows if present, capture JSON metadata
    df, metadata_json = maybe_top_metadata(df)

    cols = list(df.columns)

    # 3) pick id columns (keep plan_name separate)
    col_code1      = first_existing(cols, ["code|1", "code"])
    col_code1_type = first_existing(cols, ["code|1|type", "code_type"])
    col_code2      = first_existing(cols, ["code|2"])
    col_code2_type = first_existing(cols, ["code|2|type"])
    col_payer      = first_existing(cols, ["payer_name"])
    col_plan       = first_existing(cols, ["plan_name"])
    col_bclass     = first_existing(cols, ["billing_class"])
    col_setting    = first_existing(cols, ["setting"])
    col_currency   = first_existing(cols, ["currency"])
    col_eff_date   = first_existing(cols, ["effective_date", "start_date"])
    col_exp_date   = first_existing(cols, ["expires_on", "end_date"])
    col_modifiers  = first_existing(cols, ["modifiers"])
    col_desc       = first_existing(cols, DESC_ALIASES)
    col_drug_uom   = first_existing(cols, ["drug_unit_of_measurement"])
    col_drug_type  = first_existing(cols, ["drug_type_of_measurement"])
    col_neg_algo   = first_existing(cols, ["standard_charge|negotiated_algorithm","negotiated_algorithm"])
    col_method     = first_existing(cols, ["standard_charge|methodology","methodology"])
    col_est_amt    = first_existing(cols, ["estimated_amount"])
    col_add_notes  = first_existing(cols, ["additional_generic_notes"])

    id_cols = [x for x in [
        col_code1, col_code1_type, col_code2, col_code2_type,
        col_payer, col_plan, col_bclass, col_setting,
        col_currency, col_eff_date, col_exp_date, col_desc,
        col_modifiers, col_drug_uom, col_drug_type, col_neg_algo,
        col_method, col_est_amt, col_add_notes
    ] if x]

    # 4) detect wide price columns
    price_specs = detect_wide_price_columns(cols)  # [(colname, price_type, payer, plan), ...]
    price_cols = [c for (c, _, _, _) in price_specs]
    if not price_cols:
        print("   ⚠️  No standard_charge-like / negotiated columns found; produced 0 rows.")
        return pd.DataFrame(columns=[
            "hospital_name","code","code_type","code_2","code_2_type",
            "price_type","price_amount","payer_name","plan_name","billing_class",
            "currency","effective_date","expires_on","description","modifiers",
            "drug_unit_of_measurement","drug_type_of_measurement","negotiated_algorithm",
            "estimated_amount","methodology","additional_generic_notes","metadata"
        ])

    # 5) melt to long
    wdf = df[id_cols + price_cols].copy()
    long_df = pd.melt(
        wdf,
        id_vars=id_cols,
        value_vars=price_cols,
        var_name="price_col",
        value_name="price_amount_raw"
    )

    # 6) attach price_type and payer/plan from headers (don’t override real columns)
    header_map = {c: (ptype, payer, plan) for (c, ptype, payer, plan) in price_specs}
    long_df["price_type"] = long_df["price_col"].map(lambda c: header_map.get(c, ("", "", ""))[0])
    long_df["payer_from_header"] = long_df["price_col"].map(lambda c: header_map.get(c, ("", "", ""))[1])
    long_df["plan_from_header"]  = long_df["price_col"].map(lambda c: header_map.get(c, ("", "", ""))[2])
    long_df.drop(columns=["price_col"], inplace=True)

    # 7) coalesce codes/types
    def present(df_, names): return [n for n in names if n in df_.columns]
    code_any = present(long_df, ["code|1","code|2","code|3","code|4","code"])
    type_any = present(long_df, ["code|1|type","code|2|type","code|3|type","code|4|type","code_type"])

    long_df["code"]      = long_df.apply(lambda r: coalesce_first(r, code_any), axis=1)
    long_df["code_type"] = long_df.apply(lambda r: coalesce_first(r, type_any), axis=1)

    # keep raw slot-2 if present
    long_df["code_2"]      = long_df.get(col_code2, "")
    long_df["code_2_type"] = long_df.get(col_code2_type, "")

    # normalize common type strings
    def norm_type(t):
        t = (t or "").strip().upper()
        if t in ("CPT®","CPT-4","CPT4"): return "CPT"
        if t in ("HCPCS","HCPCS-CODE"): return "HCPCS"
        if t in ("DRG","MS-DRG","MSDRG"): return "DRG"
        if t in ("ICD","ICD10","ICD-10"): return "ICD10"
        return t
    long_df["code_type"] = long_df["code_type"].map(norm_type)

    # 8) choose payer/plan columns: prefer explicit cols; else from header inference
    long_df["payer_name"] = long_df.get(col_payer, "")
    long_df.loc[long_df["payer_name"].eq("") & long_df["payer_from_header"].astype(str).ne(""),
                "payer_name"] = long_df["payer_from_header"]
    long_df["plan_name"] = long_df.get(col_plan, "")
    long_df.loc[long_df["plan_name"].eq("") & long_df["plan_from_header"].astype(str).ne(""),
                "plan_name"] = long_df["plan_from_header"]

    # 9) price_amount cleanup
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
    long_df.drop(columns=["price_amount_raw","payer_from_header","plan_from_header"], inplace=True, errors="ignore")

    # 10) build final tall frame
    def col_or_blank(df_, name):
        return df_[name].fillna("") if name in df_.columns else pd.Series([""]*len(df_), index=df_.index)

    tall = pd.DataFrame({
        "hospital_name":             hospital_name,
        "code":                      col_or_blank(long_df, "code"),
        "code_type":                 col_or_blank(long_df, "code_type"),
        "code_2":                    col_or_blank(long_df, "code_2"),
        "code_2_type":               col_or_blank(long_df, "code_2_type"),
        "price_type":                col_or_blank(long_df, "price_type"),
        "price_amount":              col_or_blank(long_df, "price_amount"),
        "payer_name":                col_or_blank(long_df, "payer_name"),
        "plan_name":                 col_or_blank(long_df, "plan_name"),
        "billing_class":             col_or_blank(long_df, col_bclass or ""),
        "currency":                  col_or_blank(long_df, col_currency or ""),
        "effective_date":            col_or_blank(long_df, col_eff_date or ""),
        "expires_on":                col_or_blank(long_df, col_exp_date or ""),
        "description":               col_or_blank(long_df, col_desc or ""),
        "modifiers":                 col_or_blank(long_df, col_modifiers or ""),
        "drug_unit_of_measurement":  col_or_blank(long_df, col_drug_uom or ""),
        "drug_type_of_measurement":  col_or_blank(long_df, col_drug_type or ""),
        "negotiated_algorithm":      col_or_blank(long_df, col_neg_algo or ""),
        "estimated_amount":          col_or_blank(long_df, col_est_amt or ""),
        "methodology":               col_or_blank(long_df, col_method or ""),
        "additional_generic_notes":  col_or_blank(long_df, col_add_notes or ""),
        "metadata":                  pd.Series([metadata_json]*len(long_df)) if metadata_json else pd.Series([""]*len(long_df)),
        "source_hint":               pd.Series(["wide_to_tall"]*len(long_df)),
    })

    # 11) keep only usable rows (avoid blanks)
    for c in ("code","code_type","price_amount"):
        tall[c] = tall[c].astype(str).str.strip()
    keep = (tall["code"]!="") & (tall["code_type"]!="") & (tall["price_amount"]!="")
    dropped = len(tall) - int(keep.sum())
    if dropped:
        print(f"   ↳ dropped {dropped} rows missing code/code_type/price")
    return tall[keep].reset_index(drop=True)

# ---------------- MAIN -------------------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    files = sorted(glob.glob(os.path.join(IN_DIR, "*.csv")) + glob.glob(os.path.join(IN_DIR, "*.csv.gz")))
    if not files:
        print(f"No files found in {IN_DIR}")
        return

    for path in files:
        hosp = guess_hospital_name_from_filename(path)
        out_name = os.path.basename(re.sub(r"\.csv(\.gz)?$", "", path, flags=re.IGNORECASE)) + ".tall.csv"
        out_path = os.path.join(OUT_DIR, out_name)
        if not OVERWRITE and os.path.exists(out_path):
            print(f"  Skipping {out_name} (exists)")
            continue

        print(f"\n→ Processing: {os.path.basename(path)}  |  inferred hospital: {hosp}")
        df = read_csv_any(path)
        tall = to_tall(df, hosp)

        if tall.empty:
            print("  Produced zero tall rows; not writing.")
            continue

        # Write tall CSV
        tall.to_csv(out_path, index=False)
        print(f" Wrote {len(tall):,} tall rows → {out_path}")

if __name__ == "__main__":
    main()
