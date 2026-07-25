import uuid
import pandas as pd
from datetime import datetime, timezone
from src.config import DATA_FILE, raw_coll, batch_coll

# Batch Job 1: Reading and Saving Raw Data
def run_ingestion():
    print("\n[Batch Job 1] Starting Data Ingestion...")

    # Create a unique ID for this specific run of the job
    batch_id = str(uuid.uuid4())

    # Log the Start of the batch
    batch_coll.insert_one({"batch_id": batch_id, "job_name": "Job 1: Ingestion", "status": "RUNNING", "start_time": datetime.now(timezone.utc)})
    
    try:
        inserted_count = 0

        # Read the CSV file in chunks of 5000 rows
        for chunk in pd.read_csv(DATA_FILE, chunksize=5000):

            # Rename columns so they are easier to understand
            chunk = chunk.rename(columns={'device': 'device_id', 'temp': 'temperature'})
            
            # Parse the timestamp
            chunk['ts'] = chunk['ts'].astype(float)
            chunk['timestamp'] = pd.to_datetime(chunk['ts'], unit='s')
            chunk = chunk.drop(columns=['ts']) 
            
            # Add extra tracking information to know when this data was imported
            chunk['batch_id'] = batch_id
            chunk['import_time'] = datetime.now(timezone.utc)
            
            # Convert the Pandas table into a list of dictionaries (which MongoDB requires)
            records = chunk.to_dict('records')
            
            # Save to MongoDB
            try:
                # skips duplicates
                raw_coll.insert_many(records, ordered=False)
                inserted_count += len(records)
            except Exception:
                # error (found a duplicate) ignoring
                pass
        
        # Log the SUCCESS of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "SUCCESS", "end_time": datetime.now(timezone.utc)}})
        print(f"[Batch Job 1] Success! Processed {inserted_count} new records.")
        
    except Exception as error_message:
        # Log the FAILURE of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "FAILED", "end_time": datetime.now(timezone.utc), "error": str(error_message)}})
        print(f"[Batch Job 1] Failed with error: {error_message}")
        raise Exception(f"Job 1 failed: {error_message}")