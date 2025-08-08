import pandas as pd
from google.cloud import bigquery
import os

# === AUTHENTICATION ===
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "supply-chain-data-integration-e58b2d6792ca.json"

# === CONFIG ===
project_id = "supply-chain-data-integration"
dataset_id = "supply_chain"

# === FILES TO UPLOAD ===
csv_files = [
    ("data\orders.csv", "fact_orders"),
    ("data\inventory.csv", "fact_inventory")
]

# === INIT BQ CLIENT ===
client = bigquery.Client(project=project_id)

for csv_file, table_name in csv_files:
    print(f"Uploading {csv_file} to {dataset_id}.{table_name}...")

    df = pd.read_csv(csv_file)
    table_ref = f"{project_id}.{dataset_id}.{table_name}"

    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait until job finishes

    print(f"✅ Uploaded to BigQuery → {table_ref}")