import sys
from src.config import setup_database
from src import job_1_ingestion, job_2_validation, job_3_aggregation, job_4_dashboard

def main():
    print("\n--- Pipeline Execution Starting ---")
    
    try:
        # Ensure DB is ready
        setup_database()
        
        # Batch Job 1: Ingestion
        job_1_ingestion.run_ingestion()
        
        # Batch Job 2: Validation
        job_2_validation.run_validation()
        
        # Batch Job 3: Aggregation
        job_3_aggregation.run_aggregation()
        
        # Batch Job 4: Dashboard Generation
        job_4_dashboard.run_dashboard_generation()
        
        print("\n--- Pipeline Execution Finished Successfully ---")
        
    except Exception as error_message:
        print(f"\n[CRITICAL ERROR] Pipeline halted.")
        print(f"Reason: {error_message}")
        print("Operator Action Required: Please check the logs, fix the underlying issue, and restart the orchestrator.")
        print("\n--- Pipeline Execution halted with error! ---")
        sys.exit(1)

if __name__ == "__main__":
    main()