from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.models import Variable
from dag_datapelanggan.script.csv_postgres import (
    csv_to_postgres
)
from dag_datapelanggan.script.run_query_postgres import (
    insert_into_postgres  
)
from shared.utils import macro
import pendulum
import logging
import json
import os

script_dir  = os.path.dirname(__file__)
local_tz    = pendulum.timezone("Asia/Jakarta")

# Load default global config
ENV             = Variable.get("ENVIRONMENT")
OPS_TASK_DIR    = ""

# Load environment config
abs_file_path       = os.path.join(script_dir, f"config/config_{ENV}.json")
config_json         = json.load(open(abs_file_path))
config_env          = config_json["env"]
POSTGRES_DB         = config_env["postgres_db"]
PG_CONN_ID          = config_env["postgres_conn"]
TMP_DATASET         = config_env["tmp_dataset"]
STG_DATASET         = config_env["stg_dataset"]
DW_DATASET          = config_env["dw_dataset"]
PG_CONN             = config_env["pg_conn"]
DATA_LIST           = config_env["data_list"]
COLUMN_MAPPING      = json.load(open(os.path.join(script_dir, f"config/column_mapping.json")))

# Define dag constants
config_dag          = config_json["dag"]
JOB_NAME            = config_dag["job_name"]
START_DATE_YEAR     = config_dag["start_date_year"]
START_DATE_MONTH    = config_dag["start_date_month"]
START_DATE_DAY      = config_dag["start_date_day"]

# Define dag config
OWNER           = config_dag["owner"]
EMAIL           = config_dag["email"]
TAGS            = config_dag["tags"]
SCHEDULE        = config_dag["schedule"]
RETRIES         = config_dag["retries"]
RETRY_DELAY     = config_dag["retry_delay"]
CONCURRENCY     = config_dag["concurrency"]
MAX_ACTIVE_RUNS = config_dag["max_active_runs"]

DESCRIPTION = (
    """##DAG docs for """
    + JOB_NAME
    + """  

#### Available config  

There is **no available config**.
"""
)

# Define default arguments
default_args = {
    "owner"             : OWNER,
    "depends_on_past"   : False,
    "email_on_failure"  : False,
    "email_on_retry"    : False,
    "email"             : EMAIL,
    "retries"           : RETRIES,
    "retry_delay"       : timedelta(seconds=RETRY_DELAY),
    # "on_failure_callback" : callback.on_failure_callback(config_json, ENV),
}

# Define dag
with DAG(
    JOB_NAME,
    access_control={f"User": {"can_read", "can_edit"}},
    start_date=datetime(START_DATE_YEAR, START_DATE_MONTH,
                        START_DATE_DAY, tzinfo=local_tz),
    schedule=SCHEDULE,
    max_active_tasks=CONCURRENCY,
    max_active_runs=MAX_ACTIVE_RUNS,
    default_args=default_args,
    tags=TAGS,
    template_searchpath=[OPS_TASK_DIR],
    catchup=False,
    is_paused_upon_creation=True,
    user_defined_macros={"job_id_bq": macro.job_id_bq},
) as dag:

    dag.doc_md = __doc__ 
    dag.doc_md = DESCRIPTION

    start_task = EmptyOperator(task_id="start_workflow")

    with TaskGroup("load_csv_to_postgres_group") as load_csv_to_postgres_group:
        for item in DATA_LIST:
            load_csv_to_postgres = PythonOperator(
                task_id=f"load_csv_to_postgres_{item}",
                python_callable=csv_to_postgres,
                op_kwargs={
                    "script_dir":f"{script_dir}/source",
                    "item":item,
                    "pg_conn":PG_CONN,
                    "data_mapping":COLUMN_MAPPING[f"{item}"],
                    "dest_schema":TMP_DATASET,
                    "dest_table":item
                }
            )

    checkpoint_1 = EmptyOperator(task_id="checkpoint_1")
    
    with TaskGroup("insert_to_stg_group") as insert_to_stg_group:
        for item in DATA_LIST:
            insert_to_stg = PythonOperator(
                task_id=f"insert_to_stg_{item}",
                python_callable=insert_into_postgres,
                op_kwargs={
                    "pg_conn":PG_CONN,
                    "sql_insert":f"{script_dir}/sql/insert_stg_{item}",
                    "dest_schema":STG_DATASET,
                    "dest_table":item
                }
            )


    end_task = EmptyOperator(task_id="end_workflow")

    # Define task dependencies
    (
        start_task
        >> load_csv_to_postgres_group
        >> checkpoint_1
        >> insert_to_stg_group
        >> end_task
    )