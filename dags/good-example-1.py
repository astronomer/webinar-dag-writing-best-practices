from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

#Example use case: we need to execute queries for a list of states that select data for today's and yesterday's dates.
#Once the queries have been completed successfully we send an email notification.

#Variables used by tasks
states = ['CO', 'WA', 'OR']
email_to = 'kenten@astronomer.io'

#Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

#Instantiate DAG
with DAG('good_practices_dag_1',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False,
         template_searchpath='/usr/local/airflow/include' #include path to look for external files
         ) as dag:

    t0 = DummyOperator(task_id='start')  

    #Define Task Group with Postgres Queries. Loop through states provided
    with TaskGroup('covid_table_queries') as covid_table_queries:
        for state in states:
            queries = PostgresOperator(
                task_id='covid_query_{0}'.format(state),
                postgres_conn_id='postgres_default',
                sql='covid_state_query.sql',
                params={'state': "'" + state + "'"}
            )

    #Define task to send email
    send_email = EmailOperator(
        task_id='send_email',
        to=email_to,
        subject='Covid Queries DAG',
        html_content='<p>The Covid queries DAG completed successfully. <p>'
    )
    
    #Define task dependencies 
    t0 >> covid_table_queries >> send_email