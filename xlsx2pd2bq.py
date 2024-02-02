import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

PROJECT_ID = ""
BUCKET_NAME = ""
XLSX_FILE_PATH = "biz_category.xlsx"
BQ_DATASET_NAME = ""

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client(PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

# add python function to load data from pandas dataframe to bigquery table
def load_data_from_pandas_dataframe_to_bigquery_table(df, table_id):
    """Load data from pandas dataframe to bigquery table."""

    client = bigquery.Client(PROJECT_ID)
    dataset_ref = client.dataset(BQ_DATASET_NAME)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )  # Make an API request.

    job.result()  # Waits for the job to complete.

    print("Loaded {} rows.".format(job.output_rows))
    
if __name__ == '__main__':
    download_blob(BUCKET_NAME, XLSX_FILE_PATH, XLSX_FILE_PATH)

    required_tabs=['bizcategory', 'geolocation']
    for sheet in required_tabs:
        print("Processing sheet:", sheet)

        df=pd.read_excel(XLSX_FILE_PATH, sheet_name=sheet)
        df.columns=[x.lower() for x in df.columns]
        print(df.head())

        load_data_from_pandas_dataframe_to_bigquery_table(df, sheet)