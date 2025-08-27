import os
import re
from datetime import datetime
from google.cloud import storage, bigquery

# === CONFIGURATION ===
BUCKET_NAME = "all-vax-data"
RAW_TABLE_ID = "all-vax.all_vax.all-vax-raw"  # project.dataset.table
FILE_PREFIX = "ALL_VAX_"
FILE_EXTENSION = ".csv"

# === AUTHENTICATION ===
# Make sure GOOGLE_APPLICATION_CREDENTIALS env variable is set

# === Step 1: Find Latest File in GCS ===
def get_latest_csv_from_gcs():
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    pattern = re.compile(rf"{FILE_PREFIX}(\d{{4}}-\d{{2}}-\d{{2}}){FILE_EXTENSION}")
    matching_files = []

    for blob in blobs:
        match = pattern.match(blob.name)
        if match:
            matching_files.append((blob.name, match.group(1)))

    if not matching_files:
        raise FileNotFoundError("❌ No matching CSV files found in GCS bucket.")

    latest_file = max(matching_files, key=lambda x: x[1])[0]
    print(f"✅ Found latest file: {latest_file}")
    return latest_file

# === Step 2: Load into BigQuery ===
def load_csv_to_bq(gcs_uri, table_id):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()
    print(f"✅ Loaded data into {table_id}")

# === MAIN ENTRY POINT FOR DAG ===
def main():
    latest_file = get_latest_csv_from_gcs()
    gcs_uri = f"gs://{BUCKET_NAME}/{latest_file}"
    load_csv_to_bq(gcs_uri, RAW_TABLE_ID)


# === MAIN EXECUTION ===
if __name__ == "__main__":
    main()
