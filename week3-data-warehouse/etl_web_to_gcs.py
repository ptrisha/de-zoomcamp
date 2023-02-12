from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as csv.gz file"""
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local csv.gz file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int) -> int:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    dataset_url =f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    rows = len(df)
    print(f"rows: {rows}")

    path = write_local(df, dataset_file)

    write_gcs(path)

    return rows


@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2019
):
    rows_processed = []
    for month in months:
        rows = etl_web_to_gcs(year, month)
        rows_processed.append(rows)

    print(f"rows processed by month: {rows_processed}")
    print(f"total rows: {sum(rows_processed)}")


if __name__ == "__main__":
    months=list(range(1, 13))
    year = 2019
    etl_parent_flow(months=months, year=year)
