from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

# Define the DAG
with DAG(
    dag_id="nasa_apod_postgres",
    start_date=days_ago(1),
    schdedule_interval="@daily",
    catchup=False
) as dag:
    # Step 1: Create the table if it doesn't exist
    @task
    def create_table():
        # initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        # SQL query to create the table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS nasa_apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)
            );
        """ 

        # execute the SQL query
        postgres_hook.run(create_table_query)

    # Step 2: Extract data from NASA API (i.e. Astronomy Picture Of The Day (APOD) data)


    # Step 3: Transform the data (i.e. Pick the information that needs to be saved)


    # Step 4: Load the transformed data into the Postgres database

    
    # Step 5: Verify the data using DBViewer


    # Step 6: Define the task dependencies
