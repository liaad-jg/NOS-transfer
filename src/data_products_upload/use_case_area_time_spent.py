import google.auth
from google.cloud import bigquery
import pandas as pd
import numpy as np
import utils.config as config
from utils.utils import align_user_table
import shapely
from utils.bigquery_utils import upload_table_bq
from use_cases.time_in_polygon import time_spent_in_area
from use_cases.dwell_time import dwell_split, dwell_split_empty

credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_table_hourly = f"{output_dataset}.area_time_spent_hourly"
output_table_daily = f"{output_dataset}.area_time_spent_daily"

dwell_time_intervals = [0] + list(range(5*60,61*60,5*60))
dwell_time_big = [0] + [i*60 for i in [15, 30, 45, 60, 90, 120, 240, 480, 720, 1440]]

output_dataset = f"{project}.data_products"
output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe()

user_table = client.query(f"SELECT * FROM {project_dataset}.{config.table_users}").to_dataframe()

max_time = pd.Timestamp(config.end_datetime_demo)
time_delta_daily = pd.Timedelta(days=1)

to_upload_df = pd.DataFrame(columns = ["polygon_id", "datetime", "residency_status", "dwell_time_interval", "count_people"])
to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "residency_status", "dwell_time_interval", "count_people"])


for i, (poly_ind, polygon) in enumerate(output_polygons.loc[output_polygons.polygon_description.isin(["Concelho", 
                                                                                                      "Freguesia", 
                                                                                                      "Subsecção", 
                                                                                                      "Secção", 
                                                                                                      "POI"])].iterrows()):
    
    print(f"{i} ({poly_ind}) ({polygon['polygon_id']})")
    
    if i%51 == 50:
        upload_table_bq(to_upload_df, output_table_hourly, schema = {}, client = client)
        upload_table_bq(to_upload_df_daily, output_table_daily, schema = {}, client = client)
        to_upload_df = pd.DataFrame(columns = ["polygon_id", "datetime", "residency_status", "dwell_time_interval", "count_people"])
        to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "residency_status", "dwell_time_interval", "count_people"])

    pol_relevant_user_table = align_user_table(polygon, user_table)
    
    current_day = pd.Timestamp(config.start_datetime_demo_prev)
    
    while current_day < max_time:
        
        time_in_area_hourly = time_spent_in_area(current_day, shapely.wkt.loads(polygon["geography"]), project_dataset, client, hourly=True)
        
        empty_dwell = dwell_split_empty(current_day, polygon["polygon_id"], dwell_time_intervals, hourly=True)
        
        if len(time_in_area_hourly) == 0:
            to_upload_df = pd.concat([to_upload_df, empty_dwell])
        
        else:
            time_with_labels = time_in_area_hourly.merge(pol_relevant_user_table, on=config.USER_ID, how="left")
            time_with_labels.residential_status.fillna("commuter", inplace=True)
            dwell_time = time_with_labels.groupby(["residential_status", 
                                                    "datetime"]).apply(lambda x: dwell_split(x, dwell_time_intervals)).reset_index()#.drop(columns=["level_2"])

            dwell_time_total = time_in_area_hourly.groupby(["datetime"]).apply(lambda x: dwell_split(x, dwell_time_intervals)).reset_index()#.drop(columns=["level_1"])
            dwell_time_total["residential_status"] = "total"
            dwell_time = pd.concat([dwell_time.loc[:, ["datetime", "residential_status", "dwell_time_interval", "count_people"]], 
                                    dwell_time_total.loc[:, ["datetime", "residential_status", "dwell_time_interval", "count_people"]]])
            dwell_time["polygon_id"] = polygon["polygon_id"]
            dwell_time.rename(columns={"residential_status": "residency_status"}, inplace = True)
            dwell_time = dwell_time.loc[:, ["polygon_id", "datetime", "residency_status", "dwell_time_interval", "count_people"]]
            zero_padded = empty_dwell.merge(dwell_time, on = ["polygon_id", "datetime", "residency_status", "dwell_time_interval"], how = "left")
            zero_padded["count_people"] = np.fmax(zero_padded["count_people_x"], zero_padded["count_people_y"])
            to_upload_df = pd.concat([to_upload_df, zero_padded.drop(columns = ["count_people_x", "count_people_y"])])

        
        time_in_area_daily = time_spent_in_area(current_day, shapely.wkt.loads(polygon["geography"]), project_dataset, client, hourly=False)
        empty_dwell = dwell_split_empty(current_day, polygon["polygon_id"], dwell_time_big, hourly=False)
        
        if len(time_in_area_daily) == 0:
            
            to_upload_df_daily = pd.concat([to_upload_df_daily, empty_dwell])
        
        else:
            time_with_labels = time_in_area_daily.merge(pol_relevant_user_table, on=config.USER_ID, how="left")
            time_with_labels.residential_status.fillna("commuter", inplace=True)
            dwell_time = time_with_labels.groupby(["residential_status"]).apply(lambda x: dwell_split(x, dwell_time_big)).reset_index()#.drop(columns=["level_1"])

            dwell_time_total = dwell_split(time_in_area_daily, dwell_time_big)
            dwell_time_total["residential_status"] = "total"
            dwell_time = pd.concat([dwell_time.loc[:, ["residential_status", "dwell_time_interval", "count_people"]], 
                                    dwell_time_total.loc[:, ["residential_status", "dwell_time_interval", "count_people"]]])
            dwell_time["polygon_id"] = polygon["polygon_id"]
            dwell_time["date"] = str(current_day.date())
            dwell_time.rename(columns={"residential_status": "residency_status"}, inplace = True)
            dwell_time = dwell_time.loc[:, ["polygon_id", "date", "residency_status", "dwell_time_interval", "count_people"]]
            zero_padded = empty_dwell.merge(dwell_time, on = ["polygon_id", "date", "residency_status", "dwell_time_interval"], how = "left")
            zero_padded["count_people"] = np.fmax(zero_padded["count_people_x"], zero_padded["count_people_y"])
            to_upload_df_daily = pd.concat([to_upload_df_daily, zero_padded.drop(columns = ["count_people_x", "count_people_y"])])

        current_day = current_day + time_delta_daily

upload_table_bq(to_upload_df, output_table_hourly, schema = {}, client = client)
upload_table_bq(to_upload_df_daily, output_table_daily, schema = {}, client = client)