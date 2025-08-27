from google.cloud import bigquery

RAW_TABLE_ID = "all-vax.all_vax.all-vax-raw"  # project.dataset.table

def truncate_raw_table():
    client = bigquery.Client()
    query = f"TRUNCATE TABLE `{RAW_TABLE_ID}`"
    client.query(query).result()
    print(f"✅ Raw table `{RAW_TABLE_ID}` truncated.")
