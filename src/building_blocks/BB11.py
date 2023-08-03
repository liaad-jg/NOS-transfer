# to find the users living in hotel or other until a given date.
# the user whose sleep location (derived from BB2,night stay table as source table) is located in a hotel else other.
from google.cloud import bigquery
import pandas as pd

def get_users_in_polygons(client, dataset, source_table_id, polygons_table, current_day_part, residential_status_table):
   
    query = f"""(WITH centroids AS ( SELECT u.imsi, p.centroid_lat, p.centroid_lon FROM {dataset}.{source_table_id} AS u JOIN {polygons_table} AS p ON CAST(u.s2code AS STRING) = p.s2code WHERE u.night_stay_place = 'yes'), 
premise_users AS ( SELECT c.imsi, c.centroid_lat, c.centroid_lon FROM centroids AS c JOIN {polygons_table} AS p ON ST_CONTAINS(ST_BUFFER(p.geography,30), ST_GEOGPOINT(c.centroid_lon, c.centroid_lat)) WHERE p.polcode IN ('LAZ-HOT'))
SELECT DISTINCT p.imsi, p.centroid_lat, p.centroid_lon FROM premise_users AS p )"""

    hotel_df = client.query(query).to_dataframe()
    hotel_df['night_stay_establishment'] = 'hotel'

    other = f"""SELECT DISTINCT(imsi) FROM {dataset}.{source_table_id} WHERE DATE(day_part) = '{current_day_part}' AND night_stay_place = 'yes'"""
    other_df = client.query(other).to_dataframe()
    other_df['night_stay_establishment'] = 'other'
    
    un = f"""SELECT DISTINCT(imsi) FROM {dataset}.{source_table_id} WHERE DATE(day_part) = '{current_day_part}' AND imsi NOT IN (SELECT DISTINCT(imsi) FROM {dataset}.{source_table_id} WHERE DATE(day_part) = '{current_day_part}' AND night_stay_place = 'yes')"""
    un_df = client.query(un).to_dataframe()
    un_df['night_stay_establishment'] = 'undefined'
    other_un_df = other_df.append(un_df, ignore_index = True)
    
    residents = f"""SELECT DISTINCT(imsi) FROM {dataset}.{residential_status_table} WHERE DATE(day_part) = '{current_day_part}' AND residential_status = 'resident'"""
    residents_df = client.query(residents).to_dataframe()
    residents_df['night_stay_establishment'] = 'home'
    
    full_user_df = pd.merge(residents_df, other_un_df, on ='imsi', how = 'outer')
    full_user_df['night_stay_establishment'] = full_user_df['night_stay_establishment_x'].combine_first(full_user_df['night_stay_establishment_y'])
    full_user_df = full_user_df[['imsi','night_stay_establishment']]
        
    user_df = pd.merge(full_user_df, hotel_df, on='imsi', how='outer')
    user_df['night_stay_establishment'] = user_df['night_stay_establishment_y'].combine_first(user_df['night_stay_establishment_x'])
    user_df = user_df[['imsi','night_stay_establishment']]
    return user_df


def table_append(client, dataset, latest_df, night_stay_type_table, current_day_part):
    latest_df['day_part'] = current_day_part
    # Delete rows from table
    query = f"DELETE FROM {dataset}.{night_stay_type_table} WHERE 1=1"

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    client.query(query, job_config=job_config).result()

    # Load new data into table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    table_ref = client.dataset(dataset).table(night_stay_type_table)    

    job = client.load_table_from_dataframe(latest_df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    


def insert_night_stay_type(client, dataset, source_table_id, polygons_table, day, night_stay_type_table, residential_status_table):
    current_day_part = pd.Timestamp(day).date()
    new_df = get_users_in_polygons(client, dataset, source_table_id, polygons_table, current_day_part, residential_status_table)
    table_append(client, dataset, new_df, night_stay_type_table, current_day_part)

    