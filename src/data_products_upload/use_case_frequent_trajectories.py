import google.auth
from google.cloud import bigquery
import pandas as pd
import utils.config as config
from building_blocks.BB8 import process_frequent_trajectories
from utils.bigquery_utils import upload_table_bq


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_table = f"{output_dataset}.frequent_trajectories"

max_time = pd.Timestamp(config.end_datetime_demo)
time_delta_hourly = pd.Timedelta(hours=1)
time_delta_daily = pd.Timedelta(days=1)

current_day = pd.Timestamp(config.start_datetime_demo)

frequent_trajectories_columns = ['polyline', 'datetime', 'origin_polygon_id', 'destination_polygon_id', 'count_trips']

min_count_people = 2
min_count_trips = 10
residency_status = 'total'

while current_day < max_time:   
    
    next_day = current_day + time_delta_daily
    current_hour = 0
    
    df_frequent_trajectories = pd.DataFrame(columns=frequent_trajectories_columns)
    
    print(f'current_day is {current_day.date()}')
    
    while current_hour < 24:
        
        print(f'current_hour is {current_hour}')
        
        next_hour = current_hour + 1                        
                
        frequent_trajectories_day_hour = process_frequent_trajectories(min_count_people, 
                                                                       min_count_trips, 
                                                                       current_day.date(), 
                                                                       current_hour, 
                                                                       residency_status, 
                                                                       project_dataset, 
                                                                       client)
        
        if frequent_trajectories_day_hour is not None and frequent_trajectories_day_hour.shape[0] > 0:
        
            #print(f'Frequent trajectory for the day {current_day.date()} and hour {current_hour} is {frequent_trajectories_day_hour}')

            df_frequent_trajectories = pd.concat([df_frequent_trajectories, 
                                                  pd.DataFrame(frequent_trajectories_day_hour, 
                                                               columns=frequent_trajectories_columns)], 
                                                 ignore_index=True)    
       
        current_hour = next_hour
    
    if df_frequent_trajectories.shape[0] > 0:
        #print(f"The df_frequent_trajectories is: \n {df_frequent_trajectories}")
        # Upload the current_day frequent trajectories
        upload_table_bq(df_frequent_trajectories, 
                        f"{output_dataset}.{config.table_frequent_trajectories}", 
                        schema = {"polyline": "geography"}, client = client)   
    
    current_day = next_day