from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

REAL_DIR = PROJECT_DIR / "Test Data"
FAKE_DIR = PROJECT_DIR / "Sample Data"

SAMPLE_ROWS_PER_FILE = 8
RANDOM_SEED = 42

REAL_DIR.mkdir(parents=True, exist_ok=True)
FAKE_DIR.mkdir(parents=True, exist_ok=True)

first_names = ["Ava", "Noah", "Liam", "Mia", "Ethan", "Emma", "Lucas", "Olivia", "Aria", "James"]
last_names = ["Smith", "Brown", "Taylor", "Wilson", "Lee", "Martin", "Davis", "Thomas", "White", "Hall"]
streets = ["King St", "George Ave", "Lake Rd", "Station St", "Hill View", "Park Lane", "River Rd", "Main St"]
cities = ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra"]

rng = np.random.default_rng(RANDOM_SEED)

def sample_rows(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if len(df) <= n:
        return df.copy()
    return df.sample(n=n, random_state=RANDOM_SEED).reset_index(drop=True)

def mask_email(s: pd.Series) -> pd.Series:
    vals = s.dropna().astype(str).unique()
    m = {v: f"user{i+1:03d}@example.com" for i, v in enumerate(vals)}
    return s.map(lambda x: m.get(str(x), x) if pd.notna(x) else x)

def mask_phone(s: pd.Series) -> pd.Series:
    vals = s.dropna().astype(str).unique()
    m = {v: f"04{(i+1):08d}" for i, v in enumerate(vals)}  # AU-like mobile format
    return s.map(lambda x: m.get(str(x), x) if pd.notna(x) else x)

def mask_name(s: pd.Series) -> pd.Series:
    vals = s.dropna().astype(str).unique()
    m = {v: f"{first_names[i % len(first_names)]} {last_names[i % len(last_names)]}" for i, v in enumerate(vals)}
    return s.map(lambda x: m.get(str(x), x) if pd.notna(x) else x)

def mask_address(s: pd.Series) -> pd.Series:
    vals = s.dropna().astype(str).unique()
    m = {
        v: f"{100 + i} {streets[i % len(streets)]}, {cities[i % len(cities)]}"
        for i, v in enumerate(vals)
    }
    return s.map(lambda x: m.get(str(x), x) if pd.notna(x) else x)

def mask_id_like(s: pd.Series, prefix: str = "ID") -> pd.Series:
    vals = s.dropna().astype(str).unique()
    m = {v: f"{prefix}{i+1:05d}" for i, v in enumerate(vals)}
    return s.map(lambda x: m.get(str(x), x) if pd.notna(x) else x)

def anonymize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        c = col.lower()
        s = out[col]

        if pd.api.types.is_datetime64_any_dtype(s):
            continue

        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            if "email" in c:
                out[col] = mask_email(s)
            elif any(k in c for k in ["phone", "mobile", "msisdn", "contact"]):
                out[col] = mask_phone(s)
            elif any(k in c for k in ["name"]):
                out[col] = mask_name(s)
            elif any(k in c for k in ["address", "street", "city", "suburb"]):
                out[col] = mask_address(s)
            elif any(k in c for k in ["account", "customer", "subscriber", "id"]):
                out[col] = mask_id_like(s, "ID")
            # else keep original text for realism

    return out

created = 0

for src in REAL_DIR.glob("*.csv"):
    df = pd.read_csv(src)
    df_small = sample_rows(df, SAMPLE_ROWS_PER_FILE)
    df_fake = anonymize(df_small)
    dst = FAKE_DIR / src.name
    df_fake.to_csv(dst, index=False)
    print(f"Saved sample: {dst} ({len(df_fake)} rows)")
    created += 1

for src in REAL_DIR.glob("*.xlsx"):
    df = pd.read_excel(src)
    df_small = sample_rows(df, SAMPLE_ROWS_PER_FILE)
    df_fake = anonymize(df_small)
    dst = FAKE_DIR / f"{src.stem}.csv"
    df_fake.to_csv(dst, index=False)
    print(f"Saved sample: {dst} ({len(df_fake)} rows)")
    created += 1

print(f"\nDone. Files created: {created}")
print(f"Source: {REAL_DIR}")
print(f"Target: {FAKE_DIR}")