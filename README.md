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
