#!/usr/bin/env python3
"""
clean.py

Defines reusable functions for cleaning the Johns Hopkins COVID-19 time series data.
"""
import pandas as pd
from pathlib import Path


from pathlib import Path
import pandas as pd

def load_latest_csv(folder: Path, prefix: str) -> pd.DataFrame:
    print(f"[DEBUG] Searching in: {folder.resolve()}")
    
    files = []
    for f in folder.iterdir():
        print(f"[DEBUG] Found: {f.name}")
        if f.name.startswith(prefix) and f.suffix == ".csv":
            files.append(f)
    
    if not files:
        print(f"[ERROR] No files found for prefix '{prefix}' in {folder}")
        exit(1)
    
    # Sort by name and pick latest (assumes date in filename)
    latest_file = sorted(files)[-1]
    print(f"[INFO] Using file: {latest_file.name}")
    return pd.read_csv(latest_file)


def melt_to_long(df: pd.DataFrame, id_vars: list, value_name: str) -> pd.DataFrame:
    """
    Melt a wide-format DataFrame of time series into a long DataFrame.
    id_vars: list of identifier columns (e.g., ['Province/State', 'Country/Region', 'Lat', 'Long'])
    value_name: name for the melted values column ('Confirmed' or 'Deaths')
    """
    date_cols = [c for c in df.columns if c not in id_vars]
    long = df.melt(
        id_vars=id_vars,
        value_vars=date_cols,
        var_name='Date',
        value_name=value_name
    )
    long['Date'] = pd.to_datetime(long['Date'], format='%m/%d/%y')
    return long


def merge_cases_deaths(df_cases: pd.DataFrame, df_deaths: pd.DataFrame, id_vars: list) -> pd.DataFrame:
    """
    Merge cases and deaths long DataFrames on id_vars and Date.
    """
    return pd.merge(
        df_cases,
        df_deaths,
        on=id_vars + ['Date'],
        how='left'
    )


def fill_time_series(df: pd.DataFrame, id_vars: list) -> pd.DataFrame:
    """
    Ensure each location has a row for every date, forward fill missing counts, and fill remaining NaNs with zero.
    """
    all_dates = pd.DataFrame({'Date': pd.date_range(df['Date'].min(), df['Date'].max())})
    locations = df[id_vars].drop_duplicates()
    full_idx = locations.merge(all_dates, how='cross')
    df_full = pd.merge(full_idx, df, on=id_vars + ['Date'], how='left')
    df_full[['Confirmed', 'Deaths']] = (
        df_full.groupby(id_vars)[['Confirmed', 'Deaths']]
               .ffill()
               .fillna(0)
    )
    return df_full


def save_cleaned(df: pd.DataFrame, processed_dir: Path, filename: str = 'covid19_cleaned.csv') -> Path:
    """
    Save the cleaned DataFrame to processed_dir/filename.
    Returns the path where file was written.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    out_path = processed_dir / filename
    df.to_csv(out_path, index=False)
    return out_path

import pandas as pd

def reshape_and_merge(confirmed: pd.DataFrame, deaths: pd.DataFrame) -> pd.DataFrame:
    # Keep only necessary columns
    id_vars = ["Province/State", "Country/Region", "Lat", "Long"]
    value_vars = confirmed.columns.difference(id_vars)

    # Melt confirmed
    confirmed_melted = confirmed.melt(id_vars=id_vars, value_vars=value_vars,
                                      var_name="Date", value_name="Confirmed")

    # Melt deaths
    deaths_melted = deaths.melt(id_vars=id_vars, value_vars=value_vars,
                                var_name="Date", value_name="Deaths")

    # Merge them
    df = pd.merge(confirmed_melted, deaths_melted,
                  on=id_vars + ["Date"], how="outer")

    # Convert date column
    df["Date"] = pd.to_datetime(df["Date"])

    # Aggregate by country and date (optional)
    df = df.groupby(["Country/Region", "Date"], as_index=False).agg({
        "Confirmed": "sum",
        "Deaths": "sum"
    })

    return df


def fill_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["Country/Region", "Date"])
    df["Confirmed"] = df["Confirmed"].fillna(0).astype(int)
    df["Deaths"] = df["Deaths"].fillna(0).astype(int)
    return df
