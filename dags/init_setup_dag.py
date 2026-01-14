from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'snowflake_conn_id': 'snowflake_default',
}

SQL_FILES = [
    '00_ddl/01_tables.sql',
    '00_ddl/02_stages_streams.sql',
    '01_procedures/bronze/sp_load_bronze.sql',
    '01_procedures/silver/sp_load_passenger.sql',
    '01_procedures/silver/sp_load_airport.sql',
    '01_procedures/silver/sp_load_fact.sql',
    '01_procedures/gold/sp_load_gold.sql',
    '02_security/rls_policy.sql'
]

with DAG(
    '01_init_snowflake_objects',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    template_searchpath=['/opt/airflow/sql'] 
) as dag:

    previous_task = None

    for file_path in SQL_FILES:
        task_name = f"run_{file_path.split('/')[-1].replace('.sql', '')}"
        task = SnowflakeOperator(
            task_id=task_name,
            sql=file_path,
            split_statements=False, 
            session_parameters={'MULTI_STATEMENT_COUNT': 0},
            dag=dag
        )

        if previous_task:
            previous_task >> task
        previous_task = task