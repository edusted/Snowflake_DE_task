from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook 
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import shutil
from datetime import datetime

SOURCE_FOLDER = '/tmp/data'           
ARCHIVE_FOLDER = '/tmp/data/archive' 
FILE_NAME = 'airline_dataset.csv'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# --- ФУНКЦИЯ 1: ЗАГРУЗКА В BRONZE (Python вместо SnowflakeOperator) ---
def load_bronze_func(**kwargs):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    print("Clearing stage...")
    hook.run("REMOVE @UTILS.RAW_STAGE")
    
    # Загружаем файл
    file_path = os.path.join(SOURCE_FOLDER, FILE_NAME)
    print(f"Uploading file: {file_path}")
    
    put_query = f"PUT file://{file_path} @UTILS.RAW_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    hook.run(put_query)
    
    print("Copying into Bronze...")
    # ИСПРАВЛЕНИЕ ЗДЕСЬ: Добавили ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    copy_query = """
        COPY INTO BRONZE.RAW_AIRLINE_DATA
        FROM @UTILS.RAW_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'UTILS.CSV_FORMAT'
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        )
        ON_ERROR = 'ABORT_STATEMENT'
    """
    hook.run(copy_query)
    print("Bronze load complete.")

def archive_file_func(**kwargs):
    if not os.path.exists(ARCHIVE_FOLDER):
        os.makedirs(ARCHIVE_FOLDER)
        print(f"Created archive folder: {ARCHIVE_FOLDER}")
    
    source_path = os.path.join(SOURCE_FOLDER, FILE_NAME)
    
    if not os.path.exists(source_path):
        print(f"File {source_path} not found. Skipping archive.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"airline_dataset_{timestamp}.csv"
    destination_path = os.path.join(ARCHIVE_FOLDER, new_name)
    
    shutil.move(source_path, destination_path)
    print(f"SUCCESS: File moved from {source_path} to {destination_path}")

with DAG(
    '02_airline_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=f'{SOURCE_FOLDER}/{FILE_NAME}',
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=600,
        mode='poke'
    )

    load_bronze = PythonOperator(
        task_id='load_bronze',
        python_callable=load_bronze_func
    )

    load_silver_airport = SnowflakeOperator(
        task_id='load_silver_dim_airport',
        snowflake_conn_id='snowflake_default',
        sql='CALL UTILS.SP_LOAD_SILVER_DIM_AIRPORT();'
    )

    load_silver_passenger = SnowflakeOperator(
        task_id='load_silver_dim_passenger',
        snowflake_conn_id='snowflake_default',
        sql='CALL UTILS.SP_LOAD_SILVER_DIM_PASSENGER();'
    )

    load_silver_fact = SnowflakeOperator(
        task_id='load_silver_fact_flight',
        snowflake_conn_id='snowflake_default',
        sql='CALL UTILS.SP_LOAD_SILVER_FACT_FLIGHT();'
    )

    load_gold = SnowflakeOperator(
        task_id='load_gold',
        snowflake_conn_id='snowflake_default',
        sql='CALL UTILS.SP_LOAD_GOLD_AGGREGATES();'
    )

    archive_task = PythonOperator(
        task_id='archive_processed_file',
        python_callable=archive_file_func
    )

    wait_for_file >> load_bronze
    load_bronze >> [load_silver_airport, load_silver_passenger] >> load_silver_fact >> load_gold
    load_gold >> archive_task