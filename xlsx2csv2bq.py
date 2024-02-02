import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

PROJECT_ID = ""
BUCKET_NAME = ""
XLSX_FILE_PATH = "biz_category.xlsx"
BQ_DATASET_NAME = ""

# add python function to load file from google cloud storage
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

# add python function to upload csv file to google cloud storage
def upload_csv_to_gcs(bucket_name, csv_file_path, destination_blob_name):
    """Upload csv file to google cloud storage."""

    storage_client = storage.Client(PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)


    blob.upload_from_filename(csv_file_path)

    print(
        "File {} uploaded to {}.".format(
            csv_file_path, destination_blob_name
        )
    )

# add python function make bigquery table from pandas dataframe
def make_bq_table(bucket_name, csv_file_path, dataset_id, table_id):
    """Make bigquery table from pandas dataframe."""

    bigquery_client = bigquery.Client(PROJECT_ID)
    dataset_ref = bigquery_client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://{}/{}".format(bucket_name, csv_file_path)
    load_job = bigquery_client.load_table_from_uri(
        uri, dataset_ref.table(table_id), job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    print("Loaded {} rows.".format(load_job.output_rows))
    
if __name__ == '__main__':
    download_blob(BUCKET_NAME, XLSX_FILE_PATH, XLSX_FILE_PATH)

    required_tabs=['bizcategory', 'geolocation']
    for sheet in required_tabs:
        print("Processing sheet:", sheet)

        df=pd.read_excel(XLSX_FILE_PATH, sheet_name=sheet)
        df.columns=[x.lower() for x in df.columns]
        print(df.head())

        df.to_csv(sheet+'.csv', index=False)
        upload_csv_to_gcs(BUCKET_NAME, sheet+'.csv', sheet+'.csv')

        make_bq_table(BUCKET_NAME, sheet+'.csv', BQ_DATASET_NAME, sheet)
