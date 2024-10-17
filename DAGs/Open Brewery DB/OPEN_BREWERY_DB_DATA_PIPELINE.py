
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from slack_shared_functions import slack_notifier
from s3_shared_functions import read_s3_object
from open_brewery_db_functions import (
    api_data_load_to_s3,
    validate_insert
)
from airflow.models import Variable
from datetime import datetime
from airflow import DAG
import pendulum


local_tz = pendulum.timezone(Variable.get("br_timezone"))

default_args = {
    "owner": "Vinicius Ribeiro",
    "depends_on_past": False,
    "on_failure_callback": slack_notifier,
    "start_date": datetime(year=2024, month=10, day=17, tzinfo=local_tz),
}

doc_md_DAG = """

#### Description
This DAG is responsible for extracting data from the Open Brewery DB API and transforming it for further analysis.
The data is ingested into a data lake, following a layered architecture (bronze, silver, and gold layers).

- **Bronze Layer**: Raw data ingested directly from the API is stored.
- **Silver Layer**: Cleaned and transformed data for further analysis.
- **Gold Layer**: Aggregated views optimized for analytical queries.

Data quality checks are performed at various stages of the process to ensure accuracy and completeness, 
including row validation after each data insertion. The entire process is monitored, and failure notifications are sent to Slack.

#### Periodicity
- **Schedule**: This DAG is scheduled to run **daily at midnight** (`0 0 * * *`)
- **Catchup**: Disabled (No historical backfilling required.)

#### Reprocess
This DAG is designed to reprocess data as part of its own execution.

"""

dag = DAG(
    dag_id="OPEN_BREWERY_DB_DATA_PIPELINE",
    catchup=False,
    default_args=default_args, 
    schedule_interval="0 0 * * *",
    tags=["Open Brewery DB"],
    doc_md=doc_md_DAG,
)

with dag:
    # Fetching some variables once to reduce calls or lookups in Airflow's metadata database
    airflow_s3_bucket=Variable.get("airflow_s3_bucket")
    open_brewery_db_query_path=Variable.get("open_brewery_db_query_path")
    snowflake_database=Variable.get("snowflake_database")
    snowflake_conn="snowflake_conn"

    ###################################
    ########### DUMMY TASKS ###########
    ###################################
    START_WORKFLOW = DummyOperator(task_id="start_workflow")    
    END_WORKFLOW = DummyOperator(task_id="end_workflow")
    STAGE_I = DummyOperator(task_id="stage_i")
    STAGE_II = DummyOperator(task_id="stage_ii")

    ###################################
    ##### API & S3 DATASETS LOAD ######
    ###################################
    RAW_DATA_LOAD_TO_S3 = PythonOperator(
    task_id="raw_data_load_to_s3",
    python_callable=api_data_load_to_s3,
    op_kwargs={
        'url': Variable.get("open_brewery_db_url"),
        'endpoint': 'breweries',
        's3_bucket': airflow_s3_bucket,
        's3_target_path': Variable.get("open_brewery_db_api_response_path"),
        'file_name': 'open_brewery_db_response',
        'ext': 'json'        
    },
    dag=dag,
    )

    ###################################
    ####### DATA QUALITY TASKS ########
    ###################################
    BRONZE_INSERTION_VALIDATION = PythonOperator(
        task_id='validate_bronze_insertion',
        python_callable=validate_insert,
        op_kwargs={'task_id': 'raw_data_load_to_data_lake_bronze_layer', 'min_rows': 1},
    )

    SILVER_INSERTION_VALIDATION = PythonOperator(
        task_id='validate_silver_insertion',
        python_callable=validate_insert,
        op_kwargs={'task_id': 'data_transformation_to_data_lake_silver_layer', 'min_rows': 1},
    )

    ###################################
    ####### SNOWFLAKE OPERATORS #######
    ###################################
    RAW_DATA_LOAD_TO_DATA_LAKE_BRONZE_LAYER = SnowflakeOperator(
        task_id="raw_data_load_to_data_lake_bronze_layer",
        sql=read_s3_object(
            s3_bucket_name=airflow_s3_bucket,
            s3_object_path=open_brewery_db_query_path,
            object_file_name="OPEN_BREWERY_DB_BRONZE_LAYER_INSERTION.sql",
        ),
        params={
            "database_name": snowflake_database
        },
        snowflake_conn_id=snowflake_conn,
        do_xcom_push=True
    )

    DATA_TRANSFORMATION_TO_DATA_LAKE_SILVER_LAYER = SnowflakeOperator(
        task_id = "data_transformation_to_data_lake_silver_layer",
        sql=read_s3_object(
            s3_bucket_name=airflow_s3_bucket,
            s3_object_path=open_brewery_db_query_path,
            object_file_name="OPEN_BREWERY_DB_SILVER_LAYER_INSERTION.sql",
        ),
        params={
            "database_name": snowflake_database
        },
        snowflake_conn_id=snowflake_conn,
        do_xcom_push=True
    )

    AGG_VIEW_GENERATION_TO_DATA_LAKE_GOLD_LAYER = SnowflakeOperator(
        task_id = "agg_view_generation_to_data_lake_gold_layer",
        sql=read_s3_object(
            s3_bucket_name=airflow_s3_bucket,
            s3_object_path=open_brewery_db_query_path,
            object_file_name="OPEN_BREWERY_DB_GOLD_LAYER_VIEW_GENERATION.sql",
        ),
        params={
            "database_name": snowflake_database
        },
        snowflake_conn_id=snowflake_conn,
    )

START_WORKFLOW >> RAW_DATA_LOAD_TO_S3 >> RAW_DATA_LOAD_TO_DATA_LAKE_BRONZE_LAYER >> BRONZE_INSERTION_VALIDATION >> STAGE_I
STAGE_I >> DATA_TRANSFORMATION_TO_DATA_LAKE_SILVER_LAYER >> SILVER_INSERTION_VALIDATION >> STAGE_II
STAGE_II >> AGG_VIEW_GENERATION_TO_DATA_LAKE_GOLD_LAYER >> END_WORKFLOW