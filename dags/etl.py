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
    # https://api.nasa.gov/planetary/apod?api_key=V5H1G7bkDb5kBF5MgBbsAEPUCu5CGLk7AhABoc90
    extract_nasa_apod_data = SimpleHttpOperator(
        task_id="extract_nasa_apod_data",
        http_conn_id="nasa_api", # connection id defined in airflow for nasa api
        endpoint="planetary/apod", # nasa api endpoint for APOD
        method="GET",
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"}, # get api key from connection
        response_filter=lambda response: response.json(), # convert response to json
    )


    # Step 3: Transform the data (i.e. Pick the information that needs to be saved)


    # Step 4: Load the transformed data into the Postgres database

    
    # Step 5: Verify the data using DBViewer


    # Step 6: Define the task dependencies
