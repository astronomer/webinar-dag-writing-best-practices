from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta 

#Example use case: we need to refresh a materialized view in a Postgres database, then perform multiple transformations on that data.
#The data is very large (measured in gigabytes)

#Define params for Submit Run Operator
new_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'num_workers': 2,
    'node_type_id': 'i3.xlarge',
}

notebook_task = {
    'notebook_path': '/Users/kenten+001@astronomer.io/Quickstart_Notebook',
}

#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:
    
    opr_refresh_mat_view = PostgresOperator(
        task_id='refresh_mat_view',
        postgres_conn_id='postgres_default',
        sql='REFRESH MATERIALIZED VIEW example_view;',
    )

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks',
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
    opr_run_now = DatabricksRunNowOperator(
        task_id='run_now',
        databricks_conn_id='databricks',
        job_id=5,
        notebook_params=notebook_params
    )

    opr_refresh_mat_view >> opr_submit_run >> opr_run_now