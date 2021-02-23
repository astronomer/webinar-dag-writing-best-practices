# webinar-dag-writing-best-practices

This repo contains example DAGs that were used in an Astronomer webinar on DAG writing best practices. 

### Use Case 1
The first example highlights DAG design principles. The hypothetical use case is we need to execute queries for a list of states that select data for today's and yesterday's dates.
Once the queries have been completed successfully we send an email notification.

 - **bad-example-1.py** shows an example DAG to complete this use case that uses bad DAG writing practices
 - **good-example-1.py** shows an example DAG to complete this use case that uses DAG writing best practices
 
 
### Use Case 2
The second example highlights the concept of using Airflow as an orchestrator, not an execution framework. The hypothetical use case is a basic ETL; we need to refresh a materialized view in a Postgres database, then perform multiple transformations on that data,
before loading it into another table. The data is relatively large (measured in gigabytes).

 - **bad-example-2.py** shows an example DAG to complete this use case that uses bad DAG writing practices
 - **good-example-2.py** shows an example DAG to complete this use case that uses DAG writing best practices


### Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
