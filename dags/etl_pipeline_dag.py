from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
import os

default_args = {
    'owner': 'airflow',
    'snowflake_conn_id': 'snowflake_default',
}
FILE_NAME = 'airline_dataset.csv' 
LOCAL_FILE_PATH = f'/tmp/data/{FILE_NAME}'

def upload_to_stage():
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"Файл {LOCAL_FILE_PATH} не найден! Проверь папку data.")
        
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    sql = f"PUT file://{LOCAL_FILE_PATH} @UTILS.RAW_STAGE auto_compress=true overwrite=true"
    
    hook.run(sql)
    print(f"Файл {FILE_NAME} успешно загружен в Stage.")

with DAG(
    '02_airline_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/tmp/data/airline_dataset.csv',  
        poke_interval=10,
        timeout=600
    )

    upload_file = PythonOperator(
        task_id='upload_file_to_stage',
        python_callable=upload_to_stage
    )

    load_bronze = SnowflakeOperator(
        task_id='load_bronze',
        sql=f"CALL UTILS.SP_LOAD_BRONZE('{FILE_NAME}.gz')", 
    )

    load_silver_dim_passenger = SnowflakeOperator(
        task_id='load_silver_dim_passenger',
        sql="CALL UTILS.SP_LOAD_SILVER_DIM_PASSENGER()",
    )

    load_silver_dim_airport = SnowflakeOperator(
        task_id='load_silver_dim_airport',
        sql="CALL UTILS.SP_LOAD_SILVER_DIM_AIRPORT()",
    )

    load_silver_fact = SnowflakeOperator(
        task_id='load_silver_fact',
        sql="CALL UTILS.SP_LOAD_SILVER_FACT_FLIGHT()",
    )

    load_gold = SnowflakeOperator(
        task_id='load_gold',
        sql="CALL UTILS.SP_LOAD_GOLD_AGGREGATES()",
    )

    wait_for_file >> upload_file >> load_bronze
    load_bronze >> [load_silver_dim_passenger, load_silver_dim_airport] >> load_silver_fact >> load_gold