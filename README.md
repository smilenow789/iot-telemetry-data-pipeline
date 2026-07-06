# IoT Environmental Telemetry Data System


## Architecture and Technologies
- Python 3.11: Core pipeline logic and orchestration
- MongoDB: Document-oriented database for flexible schema evolution
- Docker: Containerization for reproducible environments


## Pipeline Workflow
The system runs through four strictly tracked batch jobs:
1. Job 1: Ingestion: Reads raw CSV telemetry data in chunks, converts formats, and loads it into MongoDB while preventing duplicates via unique indexes.
2. Job 2: Validation: Scans for physically impossible values (e.g., Humidity > 100%, Temperature < -50 C), isolates them into a sensor alerts collection, and removes them from the raw data pool.
3. Job 3: Aggregation: Compresses raw measurements into daily statistical summaries (average, min, max, reading counts) for each device.
4. Job 4: Dashboard: Generates a static HTML dashboard (dashboard.html) and a trend chart, applying business logic to flag data gaps (< 12.750 readings/day) and extreme heat events (> 30.0 C).

## Project Structure
```text
|-- data/
|   |-- iot_telemetry_data.csv       (Raw dataset)
|-- output/
|   |-- dashboard.html               (Generated report)
|   |-- temperature_trend.png        (Generated chart)
|-- docker-compose.yml               (Infrastructure configuration)
|-- Dockerfile                       (Environment configuration)
|-- requirements.txt                 (Python dependencies)
|-- main.py                          (Main script)
```

##  How to Run the Project
1. Ensure you have [Docker](https://www.docker.com/products/docker-desktop) installed on your system.
2. Open your terminal, navigate to the project root directory, and run: `docker compose up --build` to build the image and start the pipeline.

Docker will pull the MongoDB image, build the Python environment, connect to the database, and automatically begin batch-processing the CSV file.

### View the Results
Once the terminal outputs `Report generated successfully: output/report.html`, navigate to the `output/` folder and open `dashboard.html` in any web browser to view the generated report.

### Cleanup
The database uses a Docker volume (mongo_data) to persist the data even if the containers stop. If you want to completely wipe the database and start fresh, run: `docker compose down -v`

