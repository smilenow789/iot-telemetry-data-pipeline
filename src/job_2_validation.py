import uuid
from datetime import datetime, timezone
from src.config import raw_coll, alerts_coll, batch_coll

# Batch Job 2: Checking Data for Errors
def run_validation():
    print("\n[Batch Job 2] Scanning data for impossible values...")

    # Create a unique ID for this specific run of the job
    batch_id = str(uuid.uuid4())

    # Log the Start of the batch
    batch_coll.insert_one({"batch_id": batch_id, "job_name": "Job 2: Validation", "status": "RUNNING", "start_time": datetime.now(timezone.utc)})
    
    try:
        # Clear out old alerts to start fresh for this batch
        alerts_coll.delete_many({}) 
        
        # Define the rules
        bad_data_rules = {
            "$or": [
                {"humidity": {"$gt": 100}},     # Humidity cannot be greater than 100%
                {"temperature": {"$lt": -50}},  # Too cold (below -50 C)
                {"temperature": {"$gt": 60}},   # Too hot (above 60 C)
                {"co": {"$lt": 0}}              # Gas levels cannot be negative
            ]
        }
        
        # Find the records that break the rules
        anomalies = list(raw_coll.find(bad_data_rules))
        
        if anomalies:
            alerts_to_save = []

            # Loop through every bad record in anomalies
            for bad_record in anomalies:
                # Create a alert report for the technician
                alert = {
                    "device_id": bad_record.get("device_id"),
                    "timestamp": bad_record.get("timestamp"),
                    "issue": "Implausible Value Detected",
                    "temperature": bad_record.get("temperature"),
                    "humidity": bad_record.get("humidity")
                }
                alerts_to_save.append(alert)

            # Save all the alerts into the alerts collection
            alerts_coll.insert_many(alerts_to_save)

            # Delete the bad data from the raw collection
            raw_coll.delete_many(bad_data_rules)
            print(f"[Batch Job 2] Complete. Found, flagged, and isolated {len(alerts_to_save)} bad readings.")
        else:
            print("[Batch Job 2] Complete. No implausible values detected. No anomalies detected.")
        
        # Log the SUCCESS of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "SUCCESS", "end_time": datetime.now(timezone.utc)}})
        
    except Exception as error_message:
        # Log the FAILURE of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "FAILED", "end_time": datetime.now(timezone.utc), "error": str(error_message)}})
        print(f"[Batch Job 2] Failed with error: {error_message}")
        raise Exception(f"Job 2 failed: {error_message}")