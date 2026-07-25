import os
import uuid
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timezone
from src.config import summary_coll, alerts_coll, batch_coll, CURRENT_EXECUTION_DATE, OUTPUT_DIR

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
                # Create a list of the exact devices and dates
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
            plt.title("Historical average Temperature Trend")
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.tight_layout()
            plt.savefig(chart_path, dpi=150) 
        plt.close()

        # Write the HTML file
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
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

        # Log the SUCCESS of the batch    
        print("[Batch Job 4] Complete. Dashboard saved to output folder.")
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "SUCCESS", "end_time": datetime.now(timezone.utc)}})

    except Exception as error_message:
        # Log the FAILURE of the batch
        batch_coll.update_one({"batch_id": batch_id}, {"$set": {"status": "FAILED", "end_time": datetime.now(timezone.utc), "error": str(error_message)}})
        print(f"[Batch Job 4] Failed with error: {error_message}")
        raise Exception(f"Job 4 failed: {error_message}")