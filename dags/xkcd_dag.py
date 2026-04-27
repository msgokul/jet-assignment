from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime
from src.elt_code import ELT

elt_obj = ELT()

default_args = {
    'owner': 'admin', 
    'depends_on_past': False, 
    'start_date' : datetime(2026, 4, 27), 
    'email_on_failure': False, 
    'retries': 1
}

with DAG(
    dag_id='xkcd_dag', 
    description="XKCD comics - Extract, Load and Transform(Mon/Wed/Fri)", 
    default_args = default_args, 
    schedule_interval= "0 0 * * 1,3,5", # At 00:00 on Monday, Wednesday and Friday
    catchup= False,
    max_active_runs= 1,
    tags= ["xkcd", "elt", "dbt"]
) as dag:
    
    # Task to ensure historical data is loaded before starting the regular schedule
    ensure_historical_data_task = PythonOperator(
        task_id='ensure_historical_data',
        python_callable=elt_obj.ensure_historical_data
    )

    # Task for Polling the XKCD API for new comics using HttpSensor
    wait_for_new_comic_task = HttpSensor(
        task_id='wait_for_new_comic', 
        http_conn_id='xkcd_api', 
        endpoint='info.0.json', 
        response_check=elt_obj.check_comic_availability, 
        poke_interval=3600, # Poking after 1 hour
        timeout=86400,      # Timeout after 24 hours if no new comic is found
        mode='reschedule'
    )

    # Task for extracting and loading data
    extract_and_load_data_task = PythonOperator(
        task_id='extract_and_load_data', 
        python_callable = elt_obj.extract_and_load
    )

    # Task for running dbt transformations
    dbt_run_task = BashOperator(
        task_id='dbt_run', 
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .'
    )

    # Task for running dbt tests
    dbt_test_task = BashOperator(
        task_id='dbt_test', 
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .'
    )

    ensure_historical_data_task >> wait_for_new_comic_task >> extract_and_load_data_task >> dbt_run_task >> dbt_test_task

