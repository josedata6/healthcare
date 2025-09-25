
ALTER TABLE public.hospital_prices
  ADD COLUMN IF NOT EXISTS plan_name                  TEXT,
  ADD COLUMN IF NOT EXISTS modifiers                  TEXT,
  ADD COLUMN IF NOT EXISTS setting                    TEXT,
  ADD COLUMN IF NOT EXISTS drug_unit_of_measurement   TEXT,
  ADD COLUMN IF NOT EXISTS drug_type_of_measurement   TEXT,
  ADD COLUMN IF NOT EXISTS negotiated_algorithm       TEXT,
  ADD COLUMN IF NOT EXISTS estimated_amount           TEXT,
  ADD COLUMN IF NOT EXISTS methodology                TEXT,
  ADD COLUMN IF NOT EXISTS additional_generic_notes   TEXT,
  ADD COLUMN IF NOT EXISTS metadata                   TEXT,
  ADD COLUMN IF NOT EXISTS code_2                     TEXT,
  ADD COLUMN IF NOT EXISTS code_2_type     