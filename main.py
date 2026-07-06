import os             # To interact with the operating system
import sys            # To stop the script immediately if a critical failure occurs
import uuid           # To generate unique IDs for our batch jobs
import pandas as pd   # Pandas for working with data in tables
import matplotlib.pyplot as plt          # To draw the temperature chart
from datetime import datetime, timezone  # To handle dates and times correctly
from pymongo import MongoClient, ASCENDING # To connect to MongoDB and sort data


# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "environmental_monitoring"
DATA_FILE = "data/iot_telemetry_data.csv"
OUTPUT_DIR = "output"

# The simulated date we are executing this batch job
CURRENT_EXECUTION_DATE = "2020-07-20"

# Connect to the MongoDB server
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Define the database collections
raw_coll = db["raw_measurements"]
alerts_coll = db["sensor_alerts"]
summary_coll = db["daily_environment_summary"]
batch_coll = db["batch_executions"]

# Make sure the output folder exists before saving files there
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Setup the Database
def setup_database():
    # Indexes to searching the database faster and prevent duplicate data
    print("Setting up database rules to prevent duplicate data...")
    raw_coll.create_index([("device_id", ASCENDING), ("timestamp", ASCENDING)], unique=True)
    summary_coll.create_index([("device_id", ASCENDING), ("date", ASCENDING)], unique=True)


# Batch Job 1: Reading and Saving Raw Data
def run_ingestion():
    print("\n[Batch Job 1] Starting Data Ingestion...")
    
    # Create a unique ID for this specific run of the job
    batch_id = str(uuid.uuid4())

    # Log the Start of the batch
    batch_coll.insert_one({"batch_id": batch_id, "job_name": "Job 1: Ingestion", "status": "RUNNING", "start_time": datetime.now(timezone.utc)})
    
    try:
        inserted_count = 0
        
        # Read the CSV file in chunks of 5000 rows.
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
        print("Critical error: Halting pipeline to prevent data corruption.")
        sys.exit(1)


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
                {"humidity": {"$gt": 100}},    # Humidity cannot be greater than 100%
                {"temperature": {"$lt": -50}}, # Too cold (below -50 C)
                {"temperature": {"$gt": 60}},  # Too hot (above 60 C)
                {"co": {"$lt": 0}}             # Gas levels cannot be negative
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
            print("[Batch Job 2] Complete. Data looks clean. No anomalies detected.")
        
        # Log the SUCCESS of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "SUCCESS", "end_time": datetime.now(timezone.utc)}})
    
    except Exception as error_message:
        # Log the FAILURE of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "FAILED", "end_time": datetime.now(timezone.utc), "error": str(error_message)}})
        print(f"[Batch Job 2] Failed with error: {error_message}")
        sys.exit(1)


# Batch Job 3: Calculating Daily Summaries
def run_aggregation():
    print("\n[Batch Job 3] Crunching the numbers for daily summaries...")

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
        sys.exit(1)

# Batch Job 4: Creating the HTML Dashboard
def run_dashboard_generation():
    print("\n[Batch Job 4] Generating the dashboard...")

    # Create a unique ID for this specific run of the job
    batch_id = str(uuid.uuid4())
    
    # Log the Start of the batch
    batch_coll.insert_one({"batch_id": batch_id, "job_name": "Job 4: Dashboard", "status": "RUNNING", "start_time": datetime.now(timezone.utc)})
    
    try:
        # Summaries and alerts of the MongoDB into Pandas tables
        summary_df = pd.DataFrame(list(summary_coll.find({}, {"_id": 0})))
        alerts_df = pd.DataFrame(list(alerts_coll.find({}, {"_id": 0})))

        # Figure out maintenance status
        maintenance_message = "⚪ <strong>NO DATA:</strong> Cannot check hardware."

        if not alerts_df.empty:
            maintenance_message = "🔴 <strong>ACTION REQUIRED:</strong> Critical sensor errors found. See 'Hardware Alerts' table below."
        elif not summary_df.empty:
            # Only check for missing data on full days. Exclude the CURRENT_EXECUTION_DATE.
            full_days_df = summary_df[summary_df['date'] != CURRENT_EXECUTION_DATE]

            # Check for sensors that have fewer than 12.750 readings on a full day
            low_readings_df = full_days_df[full_days_df['reading_count'] < 12750]
            
            if not low_readings_df.empty:
                # Create a list of the exact devices and dates.
                missing_data_details = []
                for index, row in low_readings_df.iterrows():
                    detail = f"Device <strong>{row['device_id']}</strong> on {row['date']} (Only {row['reading_count']} readings)"
                    missing_data_details.append(detail)
                
                # Format the list into HTML bullet points
                bullet_points = "<br>&nbsp;&nbsp;&nbsp;• " + "<br>&nbsp;&nbsp;&nbsp;• ".join(missing_data_details)
                maintenance_message = f"🟡 <strong>WARNING: DATA GAPS DETECTED!</strong> The following sensors may have disconnected:{bullet_points}"
            else:
                maintenance_message = "🟢 <strong>SYSTEM HEALTHY:</strong> All sensors are reporting perfectly."



        # Figure out environmental status
        env_message = "⚪ <strong>NO DATA:</strong> Cannot check weather."
        
        if not summary_df.empty:
            # Find any rows where the maximum temperature crossed 30.0°C
            hot_districts_df = summary_df[summary_df['max_temp'] > 30.0]
            
            if not hot_districts_df.empty:
                # Create a list of the exact devices and dates.
                heat_details = []
                for index, row in hot_districts_df.iterrows():
                    detail = f"Device <strong>{row['device_id']}</strong> on {row['date']} reached <strong>{row['max_temp']}°C</strong>"
                    heat_details.append(detail)
                    
                # Format the list into HTML bullet points
                bullet_points = "<br>&nbsp;&nbsp;&nbsp;• " + "<br>&nbsp;&nbsp;&nbsp;• ".join(heat_details)
                env_message = f"🔴 <strong>ACTION REQUIRED: EXTREME HEAT!</strong> Evaluate cooling centers near these locations:{bullet_points}"
            else:
                env_message = "🟢 <strong>NORMAL:</strong> Weather is within normal limits. No temperatures > 30°C."


        # Prepare the data tables
        if not summary_df.empty:
            # Sort the table so it reads in chronological order
            summary_df = summary_df.sort_values(by=['date', 'device_id'])
            summary_html = summary_df.to_html(classes="data-table", index=False)
        else:
            summary_html = "<p>No data available.</p>"

        if not alerts_df.empty:
            alerts_html = alerts_df.to_html(classes="data-table", index=False)
        else:
            alerts_html = "<p><i>No broken sensors found. All readings are physically possible.</i></p>"


        # Draw the chart
        chart_path = os.path.join(OUTPUT_DIR, "temperature_trend.png")
        plt.figure(figsize=(10, 5))
        
        if not summary_df.empty:
            city_avg = summary_df.groupby('date')['avg_temp'].mean().reset_index()
            city_avg['date'] = pd.to_datetime(city_avg['date'])
            plt.plot(city_avg['date'], city_avg['avg_temp'], marker='o', color='#2980b9', linewidth=2)
            plt.title("Average Temperature Trend")
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.tight_layout()
            plt.savefig(chart_path, dpi=150) 
        plt.close()


        # Write the HTML file
        html_content = f"""
        <html>
        <head>
            <title>Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; background: #f4f7f6; padding: 20px; }}
                .container {{ background: white; padding: 30px; border-radius: 8px; max-width: 1000px; margin: auto; line-height: 1.5; }}
                h1, h2 {{ color: #2c3e50; }}
                .status-banner {{ padding: 15px; border-radius: 5px; margin-bottom: 20px; background-color: #f8f9fa; border-left: 5px solid #bdc3c7; }}
                .data-table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                .data-table th, .data-table td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                .data-table th {{ background-color: #ecf0f1; }}
                img {{ max-width: 100%; height: auto; border-radius: 5px; border: 1px solid #ddd; margin-top: 10px; }}     
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Sensor Report</h1>
                <p><em>Execution Date: {CURRENT_EXECUTION_DATE}</em></p>
                
                <h2>Environmental Officer View</h2>
                <div class="status-banner">{env_message}</div>
                <img src="temperature_trend.png" alt="Temperature Trend Chart" />
                <h3>District Data (Sorted by Date)</h3>
                {summary_html}

                <h2>Maintenance Technician View</h2>
                <div class="status-banner">{maintenance_message}</div>
                <h3>Hardware Alerts</h3>
                {alerts_html}
            </div>
        </body>
        </html>
        """
        
        # Save the HTML string to a file
        with open(os.path.join(OUTPUT_DIR, "dashboard.html"), "w") as file:
            file.write(html_content)
            
        print("[Batch Job 4] Complete. Dashboard saved to output folder.")

        # Log the SUCCESS of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "SUCCESS", "end_time": datetime.now(timezone.utc)}})

    except Exception as error_message:
        # Log the FAILURE of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "FAILED", "end_time": datetime.now(timezone.utc), "error": str(error_message)}})
        print(f"[Batch Job 4] Failed with error: {error_message}")
        sys.exit(1)

# Main routine
if __name__ == "__main__":
    print("\n--- Pipeline Execution Starting ---")
    setup_database()
    run_ingestion()
    run_validation()
    run_aggregation()
    run_dashboard_generation()
    print("\n--- Pipeline Execution Finished ---")