from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def extract(path: Path) -> pd.DataFrame:
    """Extract the data parquet file as dataframe"""
    df = pd.read_parquet(path)
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame, color: str) -> int:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    df.to_gbq(
        destination_table=f"trips_data_all.{color}_tripdata",
        project_id="steady-cascade-376200",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    rows = len(df)
    print(f"rows: {rows}")
    return rows


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = extract(path)
    rows = write_bq(df, color)
    return rows


@flow(log_prints=True)
def etl_parent_flow(
    years: list[int] = [2019, 2020], color: str = "green"
):
    rows_processed = []

    months = list(range(1,13))
    for year in years:
        for month in months:
            rows = etl_gcs_to_bq(year, month, color)
            rows_processed.append(rows)
    
    print(f"rows processed by month: {rows_processed}")
    print(f"total rows: {sum(rows_processed)}")


if __name__ == "__main__":
    years = [2019, 2020]
    color = "yellow"
    etl_parent_flow(years=years, color=color)

