from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime
from src.elt_code import check_comic_availability, extract_and_load_data_method, ensure_historical_data_method

default_args = {
    'owner': 'admin', 
    'depends_on_past': False, 
    'start_date' : datetime(2026, 4, 21), 
    'email_on_failure': False, 
    'retries': 1
}

with DAG(
    dag_id='xkcd_dag', 
    description="XKCD comics - Extract, Load and Transform(Mon/Wed/Fri)", 
    default_args = default_args, 
    schedule_interval= "0 0 * * 1,3,5",
    catchup= False,
    max_active_runs= 1,
    tags= ["xkcd", "elt", "dbt"]
) as dag:
    
    ensure_historical_data_task = PythonOperator(
        task_id='ensure_historical_data',
        python_callable=ensure_historical_data_method
    )

    wait_for_new_comic_task = HttpSensor(
        task_id='wait_for_new_comic', 
        http_conn_id='xkcd_api', 
        endpoint='info.0.json', 
        response_check=check_comic_availability, 
        poke_interval=3600, 
        timeout=86400, 
        mode='reschedule'
    )

    extract_and_load_data_task = PythonOperator(
        task_id='extract_and_load_data', 
        python_callable = extract_and_load_data_method
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run', 
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .'
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test', 
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .'
    )

    ensure_historical_data_task >> wait_for_new_comic_task >> extract_and_load_data_task >> dbt_run_task >> dbt_test_task

