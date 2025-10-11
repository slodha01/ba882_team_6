import pandas as pd
from google.cloud import storage
from datetime import datetime
import os

def upload_to_gcs(df: pd.DataFrame, bucket_name: str, folder_name: str, prefix: str = "data"):
    """
    Uploads a pandas DataFrame to a GCS bucket as a CSV file.
    Each upload is timestamped for versioning.
    """
    if df.empty:
        print(f"No data to upload for {folder_name}. Skipping...")
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name) 

    # Create a timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{folder_name}/{prefix}_{timestamp}.csv"
    blob = bucket.blob(blob_name)

    # Save DataFrame to CSV in memory
    csv_data = df.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type="text/csv")

    print(f"Uploaded {len(df)} rows to gs://{bucket_name}/{blob_name}")

