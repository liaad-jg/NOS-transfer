# to find the professional status (student or worker) of all the users until a given date.
# the user whose work location (derived from BB3, work place table as source table) is located in a university/school/institute is a student (also includes staff). Worker otherwise.
from google.cloud import bigquery
import pandas as pd
    
    
def get_users_in_polygons(client, dataset, source_table_id, polygons_table, current_day_part):
   
    query = f"""(WITH centroids AS ( SELECT u.imsi, p.centroid_lat, p.centroid_lon FROM {dataset}.{source_table_id} AS u JOIN {polygons_table} AS p ON CAST(u.s2code AS STRING) = p.s2code WHERE u.work_place = 'yes'), 
premise_users AS ( SELECT c.imsi, c.centroid_lat, c.centroid_lon FROM centroids AS c JOIN {polygons_table} AS p ON ST_CONTAINS(ST_BUFFER(p.geography,70), ST_GEOGPOINT(c.centroid_lon, c.centroid_lat)) WHERE p.polcode IN ('EDU-ESC','EDU-UNI', 'EDU-INV'))
SELECT DISTINCT p.imsi, p.centroid_lat, p.centroid_lon FROM premise_users AS p )"""

    edu_df = client.query(query).to_dataframe()
    edu_df['professional_status'] = 'student'
    
    #users with work place yes
    workers = f"""SELECT DISTINCT(imsi) FROM {dataset}.{source_table_id} WHERE DATE(day_part) = '{current_day_part}' AND work_place = 'yes'"""
    full_user_df = client.query(workers).to_dataframe()
    full_user_df['professional_status'] = 'worker'
    
    #users with work place 'no' not identified
    others = f"""SELECT DISTINCT(imsi) FROM {dataset}.{source_table_id} WHERE DATE(day_part) = '{current_day_part}' AND imsi NOT IN (SELECT DISTINCT(imsi) FROM {dataset}.{source_table_id} WHERE DATE(day_part) = '{current_day_part}' AND work_place = 'yes')"""
    other_df = client.query(others).to_dataframe()
    other_df['professional_status'] = 'undefined'
    full_user_df = full_user_df.append(other_df, ignore_index = True)
    
    user_df = pd.merge(full_user_df, edu_df, on='imsi', how='outer')
    user_df['professional_status'] = user_df['professional_status_y'].combine_first(user_df['professional_status_x'])
    user_df = user_df[['imsi','professional_status']]
    return user_df


def table_append(client, dataset, latest_df, professional_status_table, current_day_part):
    latest_df['day_part'] = current_day_part
    # Delete rows from table
    query = f"DELETE FROM {dataset}.{professional_status_table} WHERE 1=1"

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    client.query(query, job_config=job_config).result()

    # Load new data into table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    table_ref = client.dataset(dataset).table(professional_status_table)    

    job = client.load_table_from_dataframe(latest_df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    

def insert_professional_status_classification(client, dataset, source_table_id, polygons_table, day, professional_status_table):
    current_day_part = pd.Timestamp(day).date()
    new_df = get_users_in_polygons(client, dataset, source_table_id, polygons_table, current_day_part)
    table_append(client, dataset, new_df, professional_status_table, current_day_part)
