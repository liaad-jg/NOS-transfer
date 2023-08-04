import pandas as pd
from google.cloud import bigquery
import numpy as np
import utils.config as config

    
def get_night_data_profile(polygon_users_df, midnight_df):
    polygon_users_df['datetime'] = pd.to_datetime(polygon_users_df['datetime'])
    night_df = polygon_users_df[(polygon_users_df['datetime'].dt.hour >= 0) & (polygon_users_df['datetime'].dt.hour < 8)]
    
    midnight_df['datetime'] = pd.to_datetime(midnight_df['datetime'])
    night_df = night_df.append(midnight_df).reset_index(drop=True)
    
    # sort, group_by id, calculate the max amd min of time per id and place in first_seen and last_seen
    # classify national and international tourist based on mcc (on first day everyone is tourist)
    # initialise length_of_stay for first day
    night_df = night_df.sort_values(['imsi', 'datetime','mcc'], ascending=[True, True, True]).reset_index()
    night_df = night_df.groupby('imsi').agg(first_seen=('datetime', 'min'), last_seen=('datetime', 'max'), mcc= ('mcc', 'max'))
    night_df['residential_status'] = night_df['mcc'].apply(lambda x: 'national tourist' if x == '268' else 'international tourist')
    time_diff = (night_df['last_seen'] - night_df['first_seen']).dt.total_seconds() / 60
    night_df['residential_status'] = ['commuter' if diff < 30 else label for diff, label in zip(time_diff, night_df['residential_status'])]
    night_df.loc[:, 'stay_time_night'] = time_diff
    night_df['stay_time_day']=0.0
    night_df['night_count']=1.0
    night_df['day_count'] = 0.0
    night_df['length_of_stay']=1.0
    night_df['night_stay_place'] = 'other'
    night_df['day_stay_place'] = 'other'
    #night_df = night_df.drop('mcc',axis = 1)
    night_df = night_df.drop('first_seen',axis = 1)
    night_df = night_df.drop('last_seen',axis = 1)
    night_df = night_df.reset_index()
    return night_df
    
def get_day_data_profile(polygon_users_df):
    polygon_users_df['datetime'] = pd.to_datetime(polygon_users_df['datetime'])
    day_df = polygon_users_df[(polygon_users_df['datetime'].dt.hour >= 8) & (polygon_users_df['datetime'].dt.hour < 20)]
    #day_df = day_df.drop('mcc',axis = 1)
    day_df = day_df.reset_index()

    # sort, group_by id, calculate the max amd min of time per id and place in first_seen and last_seen
    # classify commuters(passerbys): found less than 3omin only ine day and casual visitors found 1day without night stay
    # initialise length_of_stay for first day
    day_df = day_df.sort_values(['imsi', 'datetime', 'mcc'], ascending=[True, True, True]).reset_index()
    day_df = day_df.groupby('imsi').agg(first_seen=('datetime', 'min'), last_seen=('datetime', 'max'), mcc= ('mcc', 'max'))
    time_diff = (day_df['last_seen'] - day_df['first_seen']).dt.total_seconds() / 60
    day_df['residential_status'] = ['commuter' if diff < 30 else 'casual visitor' for diff in time_diff]
    day_df.loc[:, 'stay_time_day'] = time_diff
    day_df['stay_time_night']=0.0
    day_df['night_count']=0.0
    day_df['day_count'] = 1.0
    day_df['length_of_stay']=1.0
    day_df['day_stay_place'] = 'other'
    day_df = day_df.reset_index()
    day_df = day_df.drop('first_seen',axis = 1)
    day_df = day_df.drop('last_seen',axis = 1)
    return day_df
    
def merge_day_night(night_df, day_df, current_day_part):
    merged_df = pd.merge(night_df, day_df, on='imsi', how='outer')
    merged_df['mcc'] = merged_df['mcc_x'].combine_first(merged_df['mcc_y'])
    merged_df['stay_time_night'] = merged_df['stay_time_night_x'].fillna(0.0)
    merged_df['stay_time_day'] = merged_df['stay_time_day_y'].fillna(0.0)
    merged_df['night_count'] = merged_df['night_count_x'].fillna(0.0)
    merged_df['day_count'] = merged_df['day_count_y'].fillna(0.0)
    merged_df['length_of_stay'] = merged_df['length_of_stay_x'].combine_first(merged_df['length_of_stay_y'])
    merged_df['residential_status'] = merged_df['residential_status_x'].combine_first(merged_df['residential_status_y'])
    
    merged_df['night_stay_place'] = 'other'
    merged_df['day_stay_place'] = 'other'
    merged_df = merged_df[night_df.columns]
    merged_df['day_part'] = current_day_part
    return merged_df

def merge_old_new_profile(new_df, old_df):
    
    # Merge the two dataframes
    merged_df = pd.merge(new_df, old_df, on='imsi', how='outer')
    merged_df['mcc'] = merged_df['mcc_x'].combine_first(merged_df['mcc_y'])
    merged_df["length_of_stay"] = merged_df["length_of_stay_x"].fillna(0.0) + merged_df["length_of_stay_y"].fillna(0.0)
    merged_df["night_count"] = merged_df["night_count_x"].fillna(0.0) + merged_df["night_count_y"].fillna(0.0)
    merged_df["day_count"] = merged_df["day_count_x"].fillna(0.0) + merged_df["day_count_y"].fillna(0.0)

    merged_df["stay_time_night"] = np.where(merged_df["stay_time_night_x"].isnull() | (merged_df["stay_time_night_x"] == 0.0), merged_df["stay_time_night_y"].fillna(0.0), (merged_df["stay_time_night_x"].fillna(0.0) + (merged_df["stay_time_night_y"].fillna(0.0)*merged_df["night_count_y"].fillna(0.0))) / merged_df["night_count"])
    
    merged_df["stay_time_day"] = np.where(merged_df["stay_time_day_x"].isnull() | (merged_df["stay_time_day_x"] == 0.0), merged_df["stay_time_day_y"].fillna(0.0), (merged_df["stay_time_day_x"].fillna(0.0) + (merged_df["stay_time_day_y"].fillna(0.0)*merged_df["day_count_y"].fillna(0.0))) / merged_df["day_count"])
    merged_df['day_part'] = new_df['day_part'][0]


    # Calculate the updated length of stay
    merged_df['night_stay_place'] = 'other'
    merged_df['day_stay_place'] = 'other'


    # Calculate the updated residential status based on conditions
    def get_res_status(row):
        length_of_stay = row['length_of_stay']
        night_count = row['night_count']
        mcc = row['mcc']
        full_res_status = row['residential_status_y']
        df_res_status = row['residential_status_x']
        stay_time_night = row['stay_time_night']
        stay_time_day = row['stay_time_day']
        
        if pd.isnull(df_res_status):
            return full_res_status
        elif pd.isnull(full_res_status):
            if length_of_stay  < 2.0 and stay_time_night < config.min_night_stay_time and stay_time_day >= 30.0:
                return 'casual visitor'
            if length_of_stay  >= 2.0 and stay_time_night < config.min_night_stay_time and stay_time_day >= 30.0:
                return 'regular visitor'
            else:     
                return df_res_status
        elif stay_time_night < 30.0 and stay_time_day < 30.0:
            return 'commuter'
        elif length_of_stay  < 2.0 and 30.0 <= stay_time_night < config.min_night_stay_time and stay_time_day < 30.0:
            return 'casual visitor'
        elif length_of_stay  >= 2.0 and 30.0 <= stay_time_night < config.min_night_stay_time and stay_time_day < 30.0:
            return 'regular visitor'
        elif length_of_stay  >= 2.0 and night_count >= 0.0 and stay_time_day >= 30.0 and stay_time_night < config.min_night_stay_time:
            return 'regular visitor'
        elif length_of_stay  < 2.0 and night_count >= 0.0 and stay_time_day >= 30.0 and stay_time_night < config.min_night_stay_time:
            return 'casual visitor'            
        elif night_count > 0.0 and night_count < 4.0 and stay_time_night >= config.min_night_stay_time:
            if mcc == '268':
                return 'national tourist'
            else:
                return 'international tourist'     
        elif night_count >= 4.0 and stay_time_night >= config.min_night_stay_time:
            return 'resident'
       
            
    merged_df['residential_status'] = merged_df.apply(get_res_status, axis=1)
    
    # Only keep the relevant columns
    merged_df = merged_df[['imsi','mcc','stay_time_night','night_count', 'stay_time_day', 'day_count', 'length_of_stay','night_stay_place','day_stay_place', 'residential_status', 'day_part']]

    # Rename the columns to match df1
    merged_df.columns = ['imsi','mcc','stay_time_night','night_count','stay_time_day', 'day_count', 'length_of_stay','night_stay_place','day_stay_place', 'residential_status', 'day_part']

    return merged_df

def update_home(one_df):
    resident_filter = (one_df['night_count'] > 0.0) & (one_df['stay_time_night'] >= config.min_night_stay_time)
    resident_df = one_df[resident_filter]

    # Filter rows with quality 'bad' and marks equal to 0.0
    other_filter = (one_df['night_count'] == 0.0) |((one_df['night_count'] > 0.0) & (one_df['stay_time_night'] < config.min_night_stay_time))
    other_df = one_df[other_filter]
    
    # Sort the DataFrame within each group by 'marks' and 'score' in descending order
    df_sorted = resident_df.groupby('imsi', group_keys=False).apply(lambda x: x.sort_values(['night_count', 'stay_time_night'], ascending=False))

    # Update label to 'pass' for the first row within each group (highest marks and score)
    df_sorted['night_stay_place'] = df_sorted.groupby('imsi').cumcount().eq(0).map({True: 'yes', False: 'other'})
    

    full_df = df_sorted.append(other_df, ignore_index=True)
    return full_df

def update_work(one_df):
    df_commute = one_df[(one_df['stay_time_day']<config.min_night_stay_time) | (one_df['day_count'] <= 2)].copy()
    df_commute.loc[:, 'day_stay_place']= 'other'

    # Sort the DataFrame within each group by 'count' and 'time' in descending order
    df_sorted = one_df.groupby('imsi', group_keys=False).apply(
    lambda x: x[(x['stay_time_day'] >= config.min_night_stay_time) & (x['day_count'] > 2)].sort_values(['day_count', 'stay_time_day'], ascending=False))
    # Update label to 'home' for the first row within each group (highe\\\\\st count and time)
    df_sorted['day_stay_place'] = df_sorted.groupby('imsi').cumcount().eq(0).map({True: 'yes', False: 'other'})

    # Reindex the DataFrame and drop the grouping level
    work_df = df_sorted.append(df_commute, ignore_index=True)

    return work_df


def update_residents(full_df):
    # Update 'status' column based on conditions
    mask = (full_df['night_stay_place'] != 'yes') & (full_df['residential_status'] == 'resident')
    full_df.loc[mask, 'residential_status'] = 'regular visitor'
    
    mask1 = (full_df['night_stay_place'] != 'yes') & (full_df['residential_status'] == 'national tourist')
    full_df.loc[mask1 & (full_df['night_count'] < 2.0) , 'residential_status'] = 'casual visitor'
    full_df.loc[mask1 & (full_df['night_count'] >= 2.0), 'residential_status'] = 'regular visitor'
    
    mask2 = (full_df['night_stay_place'] != 'yes') & (full_df['residential_status'] == 'international tourist')
    full_df.loc[mask2 & (full_df['night_count'] < 2.0) , 'residential_status'] = 'casual visitor'
    full_df.loc[mask2 & (full_df['night_count'] >= 2.0), 'residential_status'] = 'regular visitor'
    
    return full_df
    
    
def append_df(client, dataset_id, table_name, final_df):
    #Delete rows from table
    
    query = f"DELETE FROM {dataset_id}.{table_name} WHERE 1=1"

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    client.query(query, job_config=job_config).result()
    
    # Load new data into table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    table_ref = client.dataset(dataset_id).table(table_name)    

    job = client.load_table_from_dataframe(final_df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    
def get_users_in_polygon(polygon, current_day_part, raw_table_id, client):
    previous_day_part = current_day_part - pd.Timedelta(days=1)
    # Query the BigQuery table for rows with latitude and longitude inside the polygon
    query = f""" SELECT imsi, datetime, mcc FROM {raw_table_id} WHERE ST_INTERSECTS(ST_GEOGFROMWKB(ST_AsBinary(@polygon)), 
    ST_GEOGPOINT(CAST(lon AS FLOAT64), CAST(lat AS FLOAT64)))
      AND day_part = '{current_day_part}'"""
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("polygon", "GEOGRAPHY", polygon),
        ]
    )

    full_day_df = client.query(query, job_config=job_config).to_dataframe()
    
    query2 = f""" SELECT imsi, datetime, mcc FROM {raw_table_id} WHERE ST_INTERSECTS(ST_GEOGFROMWKB(ST_AsBinary(@polygon)), 
    ST_GEOGPOINT(CAST(lon AS FLOAT64), CAST(lat AS FLOAT64)))
    AND day_part = '{previous_day_part}' AND
    (EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) >= 20 AND
    EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) <= 23)"""
    
    job_config2 = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("polygon", "GEOGRAPHY", polygon),
        ]
    )

    midnight_df = client.query(query2, job_config=job_config2).to_dataframe()
    
    return full_day_df, midnight_df


def insert_user_residential_status(client, project_dataset, raw_table_id, current_day_part):

    table_name = 'user_profile_v1'
    
    # if the residential status table already exists no need to create but merge old data with new data with updatedvalues
    # one day at a time
    column_types = {'polygon_id': object, 'imsi': object,'mcc':int, 'night_count':float, 'stay_time_night':float ,'day_count':float, 
                    'stay_time_day':float, 'length_of_stay': float, 'night_stay_place': object, 'day_stay_place':object, 'residential_status': object, 'day_part': 'datetime64'}
    one_df = pd.DataFrame(columns=column_types.keys()).astype(column_types)
    
    sql = f"SELECT frcode as polygon_id, geography AS polygon_geom FROM {project_dataset}.{config.table_polygons} WHERE polygon_description = 'Freguesia' and cocode in ('0303', '1313')"
    
    shape_df = client.query(sql)
   
    for row in shape_df:
        current_day_part = pd.Timestamp(current_day_part).date()
        pid = row.polygon_id
        polygon = row.polygon_geom
        polygon_users_df, midnight_df = get_users_in_polygon(polygon, current_day_part, raw_table_id, client)
        if(len(polygon_users_df)!=0):
            night_df = get_night_data_profile(polygon_users_df, midnight_df)
            day_df = get_day_data_profile(polygon_users_df)
            new_df = merge_day_night(night_df, day_df, current_day_part)
            previous_day_part = current_day_part - pd.Timedelta(days=1)
            sql_old=f"SELECT * FROM {project_dataset}.{table_name} WHERE day_part = DATE('{previous_day_part}') AND polygon_id = '{pid}'"
            old_df= client.query(sql_old).to_dataframe()
            
            old_df.drop('polygon_id', axis = 1)
            latest_df = merge_old_new_profile(new_df, old_df)
            latest_df['polygon_id'] = pid
            
            one_df = one_df.append(latest_df, ignore_index=True)
        else:
            continue

    full_df=update_home(one_df)
    full_df_2=update_work(full_df)
    final_df= update_residents(full_df_2)
    append_df(client, project_dataset, table_name, final_df)
              
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

        
