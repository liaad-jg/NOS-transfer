# to find the professional status (student or worker) of all the users until a given date.
# the user whose work location (derived from BB3, work place table as source table) is located in a university/school/institute is a student (also includes staff). Worker otherwise.
import pandas as pd
import utils.config as config
from utils.bigquery_utils import upload_table_bq
    
    
def get_users_in_polygons(client, project_dataset, current_day_part):
   
    query = f"""(WITH centroids AS ( SELECT u.imsi, p.centroid_lat, p.centroid_lon FROM {project_dataset}.{config.table_user_work_location} AS u JOIN {project_dataset}.{config.table_polygons} AS p ON CAST(u.s2code AS STRING) = p.s2code WHERE u.work_place = 'yes'), 
premise_users AS ( SELECT c.imsi, c.centroid_lat, c.centroid_lon FROM centroids AS c JOIN {project_dataset}.{config.table_polygons} AS p ON ST_CONTAINS(ST_BUFFER(p.geography,70), ST_GEOGPOINT(c.centroid_lon, c.centroid_lat)) WHERE p.polcode IN ('EDU-ESC','EDU-UNI', 'EDU-INV'))
SELECT DISTINCT p.imsi, p.centroid_lat, p.centroid_lon FROM premise_users AS p )"""

    edu_df = client.query(query).to_dataframe()
    edu_df['professional_status'] = 'student'
    
    #users with work place yes
    workers = f"""SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_user_work_location} WHERE DATE(day_part) = '{current_day_part}' AND work_place = 'yes'"""
    full_user_df = client.query(workers).to_dataframe()
    full_user_df['professional_status'] = 'worker'
    
    #users with work place 'no' not identified
    others = f"""SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_user_work_location} WHERE DATE(day_part) = '{current_day_part}' AND imsi NOT IN (SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_user_work_location} WHERE DATE(day_part) = '{current_day_part}' AND work_place = 'yes')"""
    other_df = client.query(others).to_dataframe()
    other_df['professional_status'] = 'undefined'
    full_user_df = full_user_df.append(other_df, ignore_index = True)
    
    user_df = pd.merge(full_user_df, edu_df, on='imsi', how='outer')
    user_df['professional_status'] = user_df['professional_status_y'].combine_first(user_df['professional_status_x'])
    user_df = user_df[['imsi','professional_status']]
    return user_df


def table_append(client, project_dataset, latest_df, current_day_part):
    latest_df['day_part'] = current_day_part
    # Delete rows from table
    query = f"DELETE FROM {project_dataset}.{config.table_user_professional} WHERE 1=1"

    client.query(query).result()

    upload_table_bq(latest_df, f"{project_dataset}.{config.table_user_professional}", {}, client)
    
    

def insert_professional_status_classification(client, project_dataset, day):
    current_day_part = pd.Timestamp(day).date()
    new_df = get_users_in_polygons(client, project_dataset, current_day_part)
    table_append(client, project_dataset, new_df, current_day_part)
