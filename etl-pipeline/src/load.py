import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

DB_NAME = "covid_data"
DB_USER = "postgres"
DB_PASSWORD = "postgres" 
DB_HOST = "localhost"
DB_PORT = "5432"

def main():
    file_path = Path("data/processed/covid_global_cleaned.csv")

    if not file_path.exists():
        print(f"[ERROR] File not found: {file_path}")
        return

    print("[INFO] Loading cleaned data...")
    df = pd.read_csv(file_path)

    print("[INFO] Connecting to PostgreSQL...")
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    print("[INFO] Uploading to table: covid_stats...")
    df.to_sql("covid_stats", engine, if_exists="replace", index=False)

    print("[SUCCESS] Data uploaded successfully.")

if __name__ == "__main__":
    main()
