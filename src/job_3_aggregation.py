import uuid
from datetime import datetime, timezone
from src.config import raw_coll, summary_coll, batch_coll

# Batch Job 3: Calculating Daily Summaries
def run_aggregation():
    print("\n[Batch Job 3] Generating daily summaries...")

    # Create a unique ID for this specific run of the job
    batch_id = str(uuid.uuid4())

    # Log the Start of the batch
    batch_coll.insert_one({"batch_id": batch_id, "job_name": "Job 3: Aggregation", "status": "RUNNING", "start_time": datetime.now(timezone.utc)})
    
    try:
        # Steps MongoDB takes to process the data
        pipeline = [
            # Just YYYY-MM-DD date
            {
                "$addFields": {
                    "date_only": { "$dateToString": { "format": "%Y-%m-%d", "date": "$timestamp" } }
                }
            },
            # Group records by device and date
            {
                "$group": {
                    "_id": { "device_id": "$device_id", "date": "$date_only" },
                    "avg_temp": { "$avg": "$temperature" }, # Calculate the average temperature
                    "max_temp": { "$max": "$temperature" }, # Find the highest temperature
                    "avg_humidity": { "$avg": "$humidity" },# Calculate the average humidity
                    "reading_count": { "$sum": 1 }          # Count how many readings we got today
                }
            },
            # Clean up the output
            {
                "$project": {
                    "_id": 0, # Hide the MongoDB ID
                    "device_id": "$_id.device_id",
                    "date": "$_id.date",
                    "avg_temp": { "$round": ["$avg_temp", 2] },
                    "max_temp": { "$round": ["$max_temp", 2] },
                    "avg_humidity": { "$round": ["$avg_humidity", 2] },
                    "reading_count": 1
                }
            }
        ]
        
        # Run the pipeline on the raw data
        results = list(raw_coll.aggregate(pipeline))
        
        if results:
            # Replace old summaries with the newly calculated ones
            summary_coll.delete_many({})
            summary_coll.insert_many(results)
            print(f"[Batch Job 3] Complete. Generated {len(results)} daily summary records.")
        else:
            print("[Batch Job 3] Complete. No data found to summarize.")

        # Log the SUCCESS of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "SUCCESS", "end_time": datetime.now(timezone.utc)}})
        
    except Exception as error_message:
        # Log the FAILURE of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "FAILED", "end_time": datetime.now(timezone.utc), "error": str(error_message)}})
        print(f"[Batch Job 3] Failed with error: {error_message}")
        raise Exception(f"Job 3 failed: {error_message}")