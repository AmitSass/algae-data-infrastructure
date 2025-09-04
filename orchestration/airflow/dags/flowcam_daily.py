"""
FlowCam Daily Data Pipeline DAG

This DAG orchestrates the daily processing of FlowCam microscopy data
through the complete data pipeline: ingestion → validation → transformation → serving.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Import our custom libraries
from algae_lib.s3_io import S3Manager
from algae_lib.db_io import DatabaseManager
from algae_lib.flowcam_utils import FlowCamProcessor

# Default arguments for the DAG
default_args = {
    'owner': 'baralgae-data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    'flowcam_daily_pipeline',
    default_args=default_args,
    description='Daily FlowCam data processing pipeline',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['flowcam', 'daily', 'data-pipeline'],
)


def seed_demo_data(**context):
    """Generate and seed demo data for the pipeline."""
    try:
        from scripts.seed_demo_data import main as seed_main
        success = seed_main()
        if not success:
            raise Exception("Demo data seeding failed")
        return "Demo data seeded successfully"
    except Exception as e:
        raise Exception(f"Demo data seeding failed: {str(e)}")


def upload_to_s3(**context):
    """Upload FlowCam data to S3 (Bronze layer)."""
    try:
        s3_manager = S3Manager()
        
        # Ensure bucket exists
        if not s3_manager.ensure_bucket():
            raise Exception("Failed to ensure S3 bucket exists")
        
        # Upload demo data
        demo_file = "examples/data-sample/flowcam_sample.csv"
        if not os.path.exists(demo_file):
            raise Exception(f"Demo file not found: {demo_file}")
        
        # Generate S3 key with current date
        execution_date = context['ds']
        s3_key = f"bronze/flowcam/date={execution_date}/flowcam_sample.csv"
        
        # Upload file
        success = s3_manager.upload_file(demo_file, s3_key)
        if not success:
            raise Exception("Failed to upload file to S3")
        
        return f"Successfully uploaded to s3://{s3_manager.bucket}/{s3_key}"
    except Exception as e:
        raise Exception(f"S3 upload failed: {str(e)}")


def validate_data_quality(**context):
    """Validate data quality using Great Expectations."""
    try:
        # Run Great Expectations checkpoint
        checkpoint_name = "flowcam_checkpoint"
        ge_root_dir = "data_quality/great_expectations"
        
        # Change to GE directory and run checkpoint
        import subprocess
        result = subprocess.run([
            "great_expectations", "--v3-api", "checkpoint", "run", checkpoint_name
        ], cwd=ge_root_dir, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Great Expectations validation failed: {result.stderr}")
        
        return "Data quality validation completed successfully"
    except Exception as e:
        raise Exception(f"Data quality validation failed: {str(e)}")


def run_dbt_transformation(**context):
    """Run dbt transformations."""
    try:
        import subprocess
        
        # Change to dbt directory and run transformations
        dbt_dir = "transform/dbt"
        
        # Run dbt deps, debug, run, and test
        commands = [
            ["dbt", "deps"],
            ["dbt", "debug"],
            ["dbt", "run"],
            ["dbt", "test"]
        ]
        
        for cmd in commands:
            result = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"dbt command failed: {cmd}, Error: {result.stderr}")
        
        return "dbt transformations completed successfully"
    except Exception as e:
        raise Exception(f"dbt transformation failed: {str(e)}")


def load_to_database(**context):
    """Load processed data to database."""
    try:
        db_manager = DatabaseManager()
        
        # Test connection
        if not db_manager.test_connection():
            raise Exception("Database connection test failed")
        
        # Load demo data to database
        demo_file = "examples/data-sample/flowcam_sample.csv"
        if not os.path.exists(demo_file):
            raise Exception(f"Demo file not found: {demo_file}")
        
        # Load CSV to database
        success = db_manager.load_csv_to_table(demo_file, "flowcam_raw", if_exists="replace")
        if not success:
            raise Exception("Failed to load data to database")
        
        return "Data loaded to database successfully"
    except Exception as e:
        raise Exception(f"Database load failed: {str(e)}")


# Define tasks
seed_data_task = PythonOperator(
    task_id='seed_demo_data',
    python_callable=seed_demo_data,
    dag=dag,
)

upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

load_to_db_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

validate_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

dbt_transform_task = PythonOperator(
    task_id='run_dbt_transformation',
    python_callable=run_dbt_transformation,
    dag=dag,
)

# Define task dependencies
seed_data_task >> upload_to_s3_task >> load_to_db_task >> validate_quality_task >> dbt_transform_task
