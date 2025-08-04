import time
from google.api_core.exceptions import ServiceUnavailable

def load_to_bigquery(df, table_name):
    from google.cloud import bigquery
    from config.db_config import GCP_PROJECT, BQ_DATASET

    client = bigquery.Client(project=GCP_PROJECT)
    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{table_name}"

    for attempt in range(3):
        try:
            job = client.load_table_from_dataframe(df, table_id)
            job.result()
            print(f"Loaded {len(df)} rows to {table_id}")
            break
        except ServiceUnavailable:
            print("BigQuery temporarily unavailable. Retrying...")
            time.sleep(5)
