import utils.config as config
from google.cloud import bigquery


def upload_table_bq(df, table_name, schema = {}, client = None):
    
    print(f'Inserting {len(df)} rows and {len(df.columns)} columns to table {table_name}')
    schema_bq = [bigquery.SchemaField(key, config.schema_dictionary[schema[key]]) for key in schema]
    job_config = bigquery.LoadJobConfig(schema=schema_bq)
    job = client.load_table_from_dataframe(df, table_name, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.

