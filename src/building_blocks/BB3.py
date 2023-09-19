import pandas as pd
from google.cloud import bigquery
import numpy as np
from shapely.wkt import loads

    
def get_day_data_profile(day_df):
    day_df = day_df.sort_values(['imsi', 'datetime'], ascending=[True, True])
    print(day_df)
    day_df = day_df.groupby('imsi').agg(first_seen=('datetime', 'min'), last_seen=('datetime', 'max'))
    print(day_df)
    day_df.loc[:, 'stay_time'] = (day_df['last_seen'] - day_df['first_seen']).dt.total_seconds()/60
    print(day_df)
    day_df = day_df.drop(['first_seen','last_seen'],axis = 1)
    day_df['day_count'] = 1
    day_df['work_place'] = 'other'
    #day_df = day_df.reset_index()
    return day_df
        
def merge_old_new_profile(new_df, old_df):       
    # Merge the two dataframes
    merged_df = pd.merge(new_df, old_df, on=['s2code', 'imsi'], how='outer')

    merged_df["day_count"] = merged_df["day_count_x"].fillna(0.0) + merged_df["day_count_y"].fillna(0.0)
    
    merged_df["stay_time"] = np.where(
        merged_df["stay_time_x"].isnull() | (merged_df["stay_time_x"] == 0.0),
        merged_df["stay_time_y"].fillna(0.0),
        (merged_df["stay_time_x"].fillna(0.0) + (merged_df["stay_time_y"].fillna(0.0)*merged_df["day_count_y"].fillna(0.0))) / merged_df["day_count"]
    )

    merged_df["work_place"] = merged_df["work_place_y"].combine_first(merged_df["work_place_x"])
    
    # Only keep the relevant columns
    merged_df = merged_df[['s2code','imsi','stay_time','day_count','work_place']]
    print(merged_df)
    return merged_df

    
def update_work(one_df):
    
    df_commute = one_df[(one_df['stay_time']<120) | (one_df['day_count'] <= 2)].copy()
    df_commute.loc[:, 'work_place']= 'other'
    print((df_commute))

    # Sort the DataFrame within each group by 'count' and 'time' in descending order
    #df_sorted = one_df.groupby('imsi', group_keys=False).apply(lambda x: x.sort_values(['day_count', 'stay_time'], ascending=False))
    df_sorted = one_df.groupby('imsi', group_keys=False).apply(
    lambda x: x[(x['stay_time'] >= 120) & (x['day_count'] > 2)].sort_values(['day_count', 'stay_time'], ascending=False)
)


    # Update label to 'home' for the first row within each group (highe\\\\\st count and time)
    df_sorted['work_place'] = df_sorted.groupby('imsi').cumcount().eq(0).map({True: 'yes', False: 'other'})
    print((df_sorted))
    # Reindex the DataFrame and drop the grouping level
    #df_sorted = df_sorted.reset_index(drop=True)
    work_df = df_sorted.append(df_commute, ignore_index=True)
    print(work_df)
    return work_df

def merge_day_night_users(day_df, midnight_df):  
    #merge day and day users to get total count of users
    midnight_df['stay_time'] = 0.0
    midnight_df['day_count']=0.0
    midnight_df['work_place'] = 'other'
    
    merged_df = pd.merge(day_df, midnight_df, on=['s2code', 'imsi'], how='outer')
    merged_df['stay_time'] = merged_df['stay_time_x'].fillna(merged_df['stay_time_y'])
    merged_df['day_count'] = merged_df['day_count_x'].fillna(merged_df['day_count_y'])
    merged_df['work_place'] = merged_df['work_place_x'].fillna(merged_df['work_place_y'])

    #merged_df = merged_df.drop(['count_day', 'count_day'], axis=1)
    merged_df = merged_df[['s2code','imsi','stay_time','day_count','work_place']]
    print(merged_df)
    return merged_df
    
def load_results_to_table(client, dataset_id, destination_table_id, labelled_df, current_day_part):
    labelled_df['day_part'] = current_day_part
    #Delete rows from table
    query = f"DELETE FROM {dataset_id}.{destination_table_id} WHERE 1=1"

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    client.query(query, job_config=job_config).result()
    
    # Load new data into table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    table_ref = client.dataset(dataset_id).table(destination_table_id)    

    job = client.load_table_from_dataframe(labelled_df, table_ref, job_config=job_config)
    job.result() 
    #print("new data loaded")
    
def get_users_in_polygon(polygon_wkt, geo_df):    
    polygon = loads(polygon_wkt)
    # Filter the GeoDataFrame using the polygon
    filtered_gdf = geo_df[geo_df.within(polygon)]
    #print(filtered_gdf)
    return filtered_gdf

    
def get_data(client, source_table_id, current_day_part):
    previous_day_part = pd.to_datetime(current_day_part) - pd.Timedelta(days=1)
    
    query1 = f""" WITH s2cells AS (
              SELECT DISTINCT S2_CELLIDFROMPOINT(geography, 16) as s2code, 
              imsi
              FROM {source_table_id}
              WHERE (day_part = DATE('{previous_day_part}') AND
                    (EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) >= 18 AND
                        EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) < 24)) OR
                        (day_part = '{current_day_part}' AND
    (EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) >= 0 AND
    EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) < 8)) AND imsi = 'swMbUYC/r6BFFLeE1YO3OL+TNkusa5hqc9wNGVFjem8='
                )
                SELECT * FROM s2cells"""
    
    midnight_df = client.query(query1).to_dataframe() 
    
    query2 = f""" WITH s2cells AS (
              SELECT distinct S2_CELLIDFROMPOINT(geography, 16) as s2code, 
              imsi, datetime
              FROM {source_table_id}
              WHERE (day_part = DATE('{current_day_part}') AND
                    (EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) >= 8 AND
                        EXTRACT(HOUR FROM CAST({'datetime'} AS DATETIME)) < 18)) AND imsi = 'swMbUYC/r6BFFLeE1YO3OL+TNkusa5hqc9wNGVFjem8='
                )
                SELECT * FROM s2cells"""
    
    day_df = client.query(query2).to_dataframe()
   
    return midnight_df, day_df
    

def insert_user_stay_place(client, source_table_id, current_day_part, dataset, destination_table_id):
    current_day_part = pd.Timestamp(current_day_part).date()
    
    midnight_df, day_df = get_data(client, source_table_id, current_day_part)
    new_df = day_df.groupby('s2code').apply(lambda x:get_day_data_profile(x)).reset_index()
    
    previous_day_part = current_day_part - pd.Timedelta(days=1)
    # to manage with missing days of data
    sql_old=f"SELECT * FROM {dataset}.{destination_table_id})"
    #sql_old=f"SELECT * FROM {dataset}.{destination_table_id} WHERE day_part = DATE('{previous_day_part}')"
    old_df= client.query(sql_old).to_dataframe()
    old_df.drop('day_part', axis = 1)
    
    if(len(old_df)!=0):
        latest_df = merge_old_new_profile(new_df, old_df)
    else:
        latest_df = new_df
    
    
    labelled_df= update_work(latest_df)
    full_df = merge_day_night_users(labelled_df, midnight_df)
    
    # Get the datatypes of columns
    column_types = full_df.dtypes
    
    load_results_to_table(client, dataset, destination_table_id, full_df, current_day_part)    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        

        
