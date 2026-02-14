import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
SERVICE = "fhv"

def download_and_convert_fhv():
    data_dir = Path("data") / SERVICE
    data_dir.mkdir(exist_ok=True, parents=True)

    year = 2020
    for month in range(1, 13):
        csv_gz_filename = f"{SERVICE}_tripdata_{year}-{month:02d}.csv.gz"
        csv_gz_filepath = data_dir / csv_gz_filename

        parquet_filename = f"{SERVICE}_tripdata_{year}-{month:02d}.parquet"
        parquet_filepath = data_dir / parquet_filename

        if parquet_filepath.exists():
            print(f"Skipping {parquet_filename} (already exists)")
            continue

        url = f"{BASE_URL}/{SERVICE}/{csv_gz_filename}"
        print(f"Downloading {url}")

        r = requests.get(url, stream=True)
        r.raise_for_status()

        with open(csv_gz_filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Converting {csv_gz_filename} to Parquet...")
        con = duckdb.connect()
        con.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath.as_posix()}'))
            TO '{parquet_filepath.as_posix()}' (FORMAT PARQUET)
        """)
        con.close()

        csv_gz_filepath.unlink()
        print(f"Completed {parquet_filename}")

def load_into_duckdb():
    con = duckdb.connect("taxi_rides_ny.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")
    con.execute("""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT *
        FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)
    con.close()

if __name__ == "__main__":
    download_and_convert_fhv()
    load_into_duckdb()
    print("Done.")