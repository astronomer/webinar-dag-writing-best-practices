from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

#Example use case: we need to execute queries for a list of states that select data for today's and yesterday's dates.
#Once the queries have been completed successfully we send an email notification.

#Variables used by tasks
email_to = 'noreply@astronomer.io'
#Define today's and yesterday's date using datetime module
today = datetime.today()
yesterday = datetime.today() - timedelta(1)

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
with DAG('design_bad_practices',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:

    t0 = DummyOperator(task_id='start')  

    #Create a different task manually for each state, and put SQL query directly into DAG file
    query_1 = PostgresOperator(
        task_id='covid_query_wa',
        postgres_conn_id='postgres_default',
        sql='''with yesterday_covid_data as (
                SELECT *
                FROM covid_state_data
                WHERE date = {{ params.today }}
                AND state = 'WA'
            ),
            today_covid_data as (
                SELECT *
                FROM covid_state_data
                WHERE date = {{ params.yesterday }}
                AND state = 'WA'
            ),
            two_day_rolling_avg as (
                SELECT AVG(a.state, b.state) as two_day_avg
                FROM yesterday_covid_data a
                JOIN yesterday_covid_data b 
                ON a.state = b.state
            )
            SELECT a.state, b.state, c.two_day_avg
            FROM yesterday_covid_data a
            JOIN today_covid_data b
            ON a.state=b.state
            JOIN two_day_rolling_avg c
            ON a.state=b.two_day_avg;''',
            params={'today': today, 'yesterday':yesterday}
    )
    query_2 = PostgresOperator(
        task_id='covid_query_or',
        postgres_conn_id='postgres_default',
        sql='''with yesterday_covid_data as (
                SELECT *
                FROM covid_state_data
                WHERE date = {{ params.today }}
                AND state = 'OR'
            ),
            today_covid_data as (
                SELECT *
                FROM covid_state_data
                WHERE date = {{ params.yesterday }}
                AND state = 'OR'
            ),
            two_day_rolling_avg as (
                SELECT AVG(a.state, b.state) as two_day_avg
                FROM yesterday_covid_data a
                JOIN yesterday_covid_data b 
                ON a.state = b.state
            )
            SELECT a.state, b.state, c.two_day_avg
            FROM yesterday_covid_data a
            JOIN today_covid_data b
            ON a.state=b.state
            JOIN two_day_rolling_avg c
            ON a.state=b.two_day_avg;''',
            params={'today': today, 'yesterday':yesterday}
    )
    query_3 = PostgresOperator(
        task_id='covid_query_ca',
        postgres_conn_id='postgres_default',
        sql='''with yesterday_covid_data as (
                SELECT *
                FROM covid_state_data
                WHERE date = {{ params.today }}
                AND state = 'CA'
            ),
            today_covid_data as (
                SELECT *
                FROM covid_state_data
                WHERE date = {{ params.yesterday }}
                AND state = 'CA'
            ),
            two_day_rolling_avg as (
                SELECT AVG(a.state, b.state) as two_day_avg
                FROM yesterday_covid_data a
                JOIN yesterday_covid_data b 
                ON a.state = b.state
            )
            SELECT a.state, b.state, c.two_day_avg
            FROM yesterday_covid_data a
            JOIN today_covid_data b
            ON a.state=b.state
            JOIN two_day_rolling_avg c
            ON a.state=b.two_day_avg;''',
            params={'today': today, 'yesterday':yesterday}
    )

    #Define task to send email
    send_email = EmailOperator(
        task_id='send_email',
        to=email_to,
        subject='Covid Queries DAG',
        html_content='<p>The Covid queries DAG completed successfully. <p>'
    )
    
    #Define task dependencies using multiple methods
    t0 >> [query_1, query_2, query_3]
    query_1.set_downstream(send_email)
    query_2.set_downstream(send_email)
    send_email.set_upstream(query_3)