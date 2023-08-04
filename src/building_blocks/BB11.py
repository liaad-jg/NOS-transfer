# to find the users living in hotel or other until a given date.
# the user whose sleep location (derived from BB2,night stay table as source table) is located in a hotel else other.
import pandas as pd
import utils.config as config
from utils.bigquery_utils import upload_table_bq


def get_users_in_polygons(client, project_dataset, current_day_part):
   
    query = f"""(WITH centroids AS ( SELECT u.imsi, p.centroid_lat, p.centroid_lon FROM {project_dataset}.{config.table_user_night_location} AS u JOIN {project_dataset}.{config.table_polygons} AS p ON CAST(u.s2code AS STRING) = p.s2code WHERE u.night_stay_place = 'yes'), 
premise_users AS ( SELECT c.imsi, c.centroid_lat, c.centroid_lon FROM centroids AS c JOIN {project_dataset}.{config.table_polygons} AS p ON ST_CONTAINS(ST_BUFFER(p.geography,30), ST_GEOGPOINT(c.centroid_lon, c.centroid_lat)) WHERE p.polcode IN ('LAZ-HOT'))
SELECT DISTINCT p.imsi, p.centroid_lat, p.centroid_lon FROM premise_users AS p )"""

    hotel_df = client.query(query).to_dataframe()
    hotel_df['night_stay_establishment'] = 'hotel'

    other = f"""SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_user_night_location} WHERE DATE(day_part) = '{current_day_part}' AND night_stay_place = 'yes'"""
    other_df = client.query(other).to_dataframe()
    other_df['night_stay_establishment'] = 'other'
    
    un = f"""SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_user_night_location} WHERE DATE(day_part) = '{current_day_part}' AND imsi NOT IN (SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_user_night_location} WHERE DATE(day_part) = '{current_day_part}' AND night_stay_place = 'yes')"""
    un_df = client.query(un).to_dataframe()
    un_df['night_stay_establishment'] = 'undefined'
    other_un_df = other_df.append(un_df, ignore_index = True)
    
    residents = f"""SELECT DISTINCT(imsi) FROM {project_dataset}.{config.table_users} WHERE DATE(day_part) = '{current_day_part}' AND residential_status = 'resident'"""
    residents_df = client.query(residents).to_dataframe()
    residents_df['night_stay_establishment'] = 'home'
    
    full_user_df = pd.merge(residents_df, other_un_df, on ='imsi', how = 'outer')
    full_user_df['night_stay_establishment'] = full_user_df['night_stay_establishment_x'].combine_first(full_user_df['night_stay_establishment_y'])
    full_user_df = full_user_df[['imsi','night_stay_establishment']]
        
    user_df = pd.merge(full_user_df, hotel_df, on='imsi', how='outer')
    user_df['night_stay_establishment'] = user_df['night_stay_establishment_y'].combine_first(user_df['night_stay_establishment_x'])
    user_df = user_df[['imsi','night_stay_establishment']]
    return user_df
    

def table_append(client, project_dataset, latest_df, current_day_part):
    latest_df['day_part'] = current_day_part
    # Delete rows from table
    query = f"DELETE FROM {project_dataset}.{config.table_night_stay_type} WHERE 1=1"

    client.query(query).result()

    upload_table_bq(latest_df, f"{project_dataset}.{config.table_night_stay_type}", {}, client)
    

def insert_night_stay_type(client, project_dataset, day):
    current_day_part = pd.Timestamp(day).date()
    new_df = get_users_in_polygons(client, project_dataset, current_day_part)
    table_append(client, project_dataset, new_df, current_day_part)

    