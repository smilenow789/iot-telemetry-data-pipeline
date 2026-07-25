import os
from pymongo import MongoClient, ASCENDING

# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "environmental_monitoring"
DATA_FILE = "data/iot_telemetry_data.csv"
OUTPUT_DIR = "output"

# The simulated date we are executing this batch job
# For productive environment: CURRENT_EXECUTION_DATE = datetime.now(timezone.utc).strftime('%Y-%m-%d')
CURRENT_EXECUTION_DATE = "2020-07-20"

# Connect to the MongoDB server
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Define the database collections
raw_coll = db["raw_measurements"]
alerts_coll = db["sensor_alerts"]
summary_coll = db["daily_environment_summary"]
batch_coll = db["batch_executions"]

# Make sure the output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def setup_database():
    # Indexes to searching the database faster and prevent duplicate data
    print("Setting up database rules to prevent duplicate data...")
    raw_coll.create_index([("device_id", ASCENDING), ("timestamp", ASCENDING)], unique=True)
    summary_coll.create_index([("device_id", ASCENDING), ("date", ASCENDING)], unique=True)