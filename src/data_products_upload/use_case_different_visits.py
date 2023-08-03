import google.auth
from google.cloud import bigquery
import pandas as pd
import utils.config as config
from utils.utils import align_user_table
from utils.bigquery_utils import upload_table_bq
from use_cases.different_visits import get_different_visits_query, get_always_inside_query, get_different_visits_dist

credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_table = f"{output_dataset}.area_different_visits"


output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe()

user_table = client.query(f"SELECT * FROM {project_dataset}.{config.table_users}").to_dataframe()

interval_lengths = [1, 2, 5, 7]

max_time = pd.to_datetime(config.end_datetime_demo_prev, utc=True)

to_upload_df = pd.DataFrame(columns = ["polygon_id", "date_start", "interval_length", "date_end", 
                                       "residency_status", "number_different_visits", "count_people"])

for i, (poly_ind, polygon) in enumerate(output_polygons.loc[output_polygons.polygon_description.isin(["Concelho", 
                                                                                                      "Freguesia", 
                                                                                                      "POI", 
                                                                                                      "Secção", 
                                                                                                      "Subsecção"])].iterrows()):
        
    print(f"{i} ({poly_ind})")
    if i%101 == 100 and len(to_upload_df) > 0:
        upload_table_bq(to_upload_df, output_table, schema = {"polygon_id":"string"}, client = client)
        to_upload_df = pd.DataFrame(columns = ["polygon_id", "date_start", "interval_length", "date_end", 
                                       "residency_status", "number_different_visits", "count_people"])
        
    pol_relevant_user_table = align_user_table(polygon, user_table)
    
    current_day = pd.to_datetime(config.start_datetime_demo, utc=True)
    all_diff_visits = client.query(get_different_visits_query(polygon["geography"], project_dataset)).to_dataframe()
    
    while current_day < max_time:
        
        for interval_length in interval_lengths:
            end_day = current_day + pd.Timedelta(days = interval_length)
            
            interval_visits = all_diff_visits.loc[(all_diff_visits.time_out >= current_day) & 
                                                  (all_diff_visits.time_in < end_day), :]
            
            diff_visits_dist = get_different_visits_dist(polygon["geography"], interval_visits, current_day, end_day, pol_relevant_user_table, project_dataset, client)
            
            diff_visits_dist["polygon_id"] = polygon["polygon_id"]
            diff_visits_dist["date_start"] = str(current_day.date())
            diff_visits_dist["interval_length"] = interval_length
            diff_visits_dist["date_end"] = str(end_day.date())
            
            diff_visits_dist.rename(columns = {"residential_status": "residency_status"}, inplace=True)
            to_upload_df = pd.concat([to_upload_df, diff_visits_dist.loc[:, ["polygon_id", "date_start", "interval_length", "date_end", 
                                                    "residency_status", "number_different_visits", "count_people"]]])
        
        current_day = current_day + pd.Timedelta(days = 1)


upload_table_bq(to_upload_df, output_table, schema = {"polygon_id":"string"}, client = client)
                                      