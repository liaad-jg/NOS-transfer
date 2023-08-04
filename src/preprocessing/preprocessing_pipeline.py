import pandas as pd
import google.auth
from google.cloud import bigquery
import sys
from building_blocks import BB2, BB3, BB4, BB6, BB9, BB10, BB11
import utils.config as config
import pickle as pkl
from utils.bigquery_utils import upload_table_bq


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
    # Query the database for data, separate into trajectories and upload to details and summary tables on bigquery
    finished = insert_trajectories.insert_trajectories(day, this_hour, next_hour, demo_table, project_dataset, client)
    if len(finished) > 0:
        # if there are any finished trajectories, calculate their stay points and upload to bigquery
        insert_stay_points.insert_stay_points(finished, project_dataset, client)

# find where users are staying during the night
BB2.insert_user_stay_place(client, f"{project_dataset}.{config.table_trajectory_details}", day, project_dataset, f"{project_dataset}{config.table_user_night_location}")
# find where users are staying during the day
BB3.insert_user_stay_place(client, f"{project_dataset}.{config.table_trajectory_details}", day, project_dataset, f"{project_dataset}{config.table_user_work_location}")

# determine the mode of transport of trajectories during this day
classifier = pkl.load(open(config.mode_of_transport_model, "rb"))
stationary_trajectories = BB4.get_stationary_trajectories(client, project_dataset, day)
upload_table_bq(stationary_trajectories, f"{project_dataset}.{config.table_mode_of_transport}", {}, client)
undefined_trajectories = BB4.get_undefined_trajectories(client, project_dataset, day)
upload_table_bq(undefined_trajectories, f"{project_dataset}.{config.table_mode_of_transport}", {}, client)
for hour in range(0, 23):
    this_hour = f"{day} {hour:02d}:00:00"
    next_hour = str(pd.Timestamp(this_hour) + pd.Timedelta(hours=1))
    labeled_trajectories = BB4.assign_transport_mode_label(this_hour, next_hour, classifier, project_dataset, client)
    if labeled_trajectories is not None:
        upload_table_bq(labeled_trajectories, f"{project_dataset}.{config.table_mode_of_transport}", {}, client)

# assign a residential status to users
BB9.insert_user_residential_status(client, project_dataset, f"{project_dataset}.{config.table_trajectory_details}", day)
# assign a professional status to users
BB10.insert_professional_status_classification(client, project_dataset, day)
# classify where users are spending the night (hotel, home, other)
BB11.insert_night_stay_type(client, project_dataset, day)

# Upload the origin-destination matrix 
for hour in range(0, 23):
    this_hour = f"{day} {hour:02d}:00:00"
    next_hour = str(pd.Timestamp(this_hour) + pd.Timedelta(hours=1))
    BB6.BB6(this_hour, next_hour, project_dataset, client)
    BB6.BB6_section(this_hour, next_hour, project_dataset, client)