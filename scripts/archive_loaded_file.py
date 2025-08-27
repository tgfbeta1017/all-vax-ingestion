from google.cloud import storage
import re

BUCKET_NAME = "all-vax-data"
ARCHIVE_PREFIX = "Archive/"
FILE_PREFIX = "ALL_VAX_"
FILE_EXTENSION = ".csv"

def move_to_archive():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # Match most recent file with date pattern
    pattern = re.compile(rf"{FILE_PREFIX}(\d{{4}}-\d{{2}}-\d{{2}}){FILE_EXTENSION}")
    blobs = list(bucket.list_blobs())

    matching_files = [
        blob.name for blob in blobs if pattern.fullmatch(blob.name)
    ]
    if not matching_files:
        raise FileNotFoundError("❌ No matching ALL_VAX_yyyy-mm-dd.csv files to archive.")

    latest_file = max(matching_files)
    archive_name = ARCHIVE_PREFIX + "archive_" + latest_file.split("/")[-1]

    # Copy then delete original
    source_blob = bucket.blob(latest_file)
    bucket.copy_blob(source_blob, bucket, archive_name)
    source_blob.delete()

    print(f"✅ Archived {latest_file} → {archive_name}")
