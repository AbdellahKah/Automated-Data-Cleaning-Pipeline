from pathlib import Path
from src import clean

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

def main():
    print("[INFO] Loading confirmed cases...")
    confirmed = clean.load_latest_csv(RAW_DIR, "confirmed")
    
    print("[INFO] Loading deaths...")
    deaths = clean.load_latest_csv(RAW_DIR, "deaths")

    print("[INFO] Transforming data...")
    df = clean.reshape_and_merge(confirmed, deaths)
    df = clean.fill_missing_data(df)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_file = PROCESSED_DIR / "covid_global_cleaned.csv"
    df.to_csv(output_file, index=False)
    
    print(f"[INFO] Cleaned data saved to: {output_file}")

if __name__ == "__main__":
    main()
