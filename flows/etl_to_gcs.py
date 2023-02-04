from prefect import flow, task
from prefect_gcp import GcsBucket
import pandas as pd
from pathlib import Path
import os
from prefect.filesystems import GitHub


@task(retries=3, log_prints=True)
def get_data_from_url(url: str) -> pd.DataFrame:
    """extract tabular data from url"""
    df_green_taxi = pd.read_csv(url)
    return df_green_taxi


@task()
def convert_df_to_local_csv_file(df: pd.DataFrame, color: str, data_file: str) -> Path:
    """convert the df to csv file"""
    new_path = f"{os.getcwd()}\data\{color}"
    if not os.path.exists(new_path):
        os.makedirs(new_path)
        path = Path(f"data/{data_file}.csv")
        df.to_csv(path)
        return path
    else:
        path = Path(f"data/{data_file}.csv")
        df.to_csv(path)
        return path


@task()
def upload_csv_to_gcs(path: Path) -> None:
    """upload local csv to gcs"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("upload-to-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)


@flow(log_prints=True)
def etl_to_gcs():
    color = "green"
    year = 2020
    month = 11
    data_file = f"{color}_tripdata_{year}-0{month}"
    data_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month}.csv.gz"
    print(data_url)
    df = get_data_from_url(data_url)
    file_path = convert_df_to_local_csv_file(df, color, data_file)
    upload_csv_to_gcs(file_path)
    print(file_path)
    print(df.shape)
    print('hello from githun repo')

if __name__ == "__main__":
    etl_to_gcs()
