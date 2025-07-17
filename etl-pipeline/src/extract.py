#!/usr/bin/env python3
"""
extract.py

Fetch Johns Hopkins COVID-19 time series for confirmed cases and deaths,
saving raw CSVs to data/raw/.
"""

import requests
from datetime import datetime, timezone
from pathlib import Path

# Full raw URLs to the JHU CSSEGISandData CSVs
URLS = {
    'confirmed': (
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/'
        'master/csse_covid_19_data/csse_covid_19_time_series/'
        'time_series_covid19_confirmed_global.csv'
    ),
    'deaths': (
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/'
        'master/csse_covid_19_data/csse_covid_19_time_series/'
        'time_series_covid19_deaths_global.csv'
    )
}

RAW_DIR = Path(__file__).resolve().parents[1] / 'data' / 'raw'

def fetch_csv(name: str, url: str) -> Path:
    """
    Download a CSV from `url` and save to RAW_DIR/<name>_<timestamp>.csv.
    Returns the path to the saved file.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    filename = RAW_DIR / f'{name}_{ts}.csv'
    print(f'Downloading {name} data from {url}...')
    resp = requests.get(url)
    resp.raise_for_status()
    filename.write_bytes(resp.content)
    print(f'  → saved to {filename}')
    return filename

def main():
    for name, url in URLS.items():
        try:
            fetch_csv(name, url)
        except Exception as e:
            print(f'Error fetching {name}: {e}', file=sys.stderr)
            sys.exit(1)
    print('All files downloaded successfully.')

if __name__ == '__main__':
    main()
