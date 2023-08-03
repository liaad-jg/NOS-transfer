import pandas as pd
import google.auth
from google.cloud import bigquery
import sys


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

demo_table = "cityanalyser-busdp-p-238755.data_products.df_city_analyser_demo"

import insert_trajectories
import insert_stay_points

day = sys.argv[1]

for hour in range(0,23):
    this_hour = f"{day} {hour:02d}:00:00"
    print(this_hour)
    next_hour = str(pd.Timestamp(this_hour) + pd.Timedelta(hours=1))
    finished = insert_trajectories.insert_trajectories(day, this_hour, next_hour, demo_table, project_dataset, client)
    if len(finished) > 0:
        insert_stay_points.insert_stay_points(finished, project_dataset, client)
        
