from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.version import version
from datetime import datetime, timedelta
import pandas as pd

#Example use case: we need to refresh a materialized view in a Postgres database, then perform multiple transformations on that data.
#The data is large (measured in gigabytes)
    

def data_processing_func(**kwargs):
    #Refresh materialized view
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('REFRESH MATERIALIZED VIEW example_view;')

    #Read data into huge dataframe
    df = pd.read_sql('SELECT * FROM example_view', conn)

    #Process data in-memory using Pandas
    pivot_df = df.pivot(index='DATE', columns='STATE', values='POSITIVE').reset_index()
    pivot_df2 = df.pivot(index='DATE', columns='STATE', values='NEGATIVE').reset_index()


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('bad_practices_dag_2',
         start_date=datetime.today(),
         max_active_runs=3,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False 
         ) as dag:
    
    #One single task for the entire DAG
    opr_process_data = PythonOperator(
        task_id='data_processing',
        python_callable=data_processing_func
    )