import pandas as pd
import google.auth
from google.cloud import bigquery
import numpy as np
from collections import Counter
import time
import utils.config as config


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

demo_table = "cityanalyser-busdp-p-238755.data_products.df_city_analyser_demo"

import insert_trajectories_braga
import insert_stay_points_braga

output_dataset = f"{project}.data_products"
output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons").to_dataframe()

braga_polygon = output_polygons.loc[(output_polygons.polygon_id == '0303') & 
                                    (output_polygons.polygon_description == "Concelho")].iloc[0].geography

povoa_polygon = output_polygons.loc[(output_polygons.polygon_id == '1313') & 
                                    (output_polygons.polygon_description == "Concelho")].iloc[0].geography

day = sys.argv[1]

for hour in range(0,24):
    this_hour = f"{day} {hour:02d}:00:00"
    print(this_hour)
    next_hour = str(pd.Timestamp(this_hour) + pd.Timedelta(hours=1))
    finished = insert_trajectories_braga.insert_trajectories(day, this_hour, next_hour, demo_table, project_dataset, client, braga_polygon, povoa_polygon)
    if len(finished) > 0:
        insert_stay_points_braga.insert_stay_points(finished, project_dataset, client)
        