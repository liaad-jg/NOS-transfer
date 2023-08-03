import google.auth
from google.cloud import bigquery
import pandas as pd
import utils.config as config
from utils.utils import align_user_table
from utils.bigquery_utils import upload_table_bq
from use_cases import place_of_origin

credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_table_hourly = f"{output_dataset}.area_place_of_origin_hourly"
output_table_daily = f"{output_dataset}.area_place_of_origin_daily"

output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe()

user_table = client.query(f"SELECT * FROM {project_dataset}.{config.table_users}").to_dataframe()

to_upload_df = pd.DataFrame(columns = ["polygon_id", "datetime", "residency_status", "place_of_origin", "count_people"])
to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "residency_status", "place_of_origin", "count_people"])

for i, (poly_ind, polygon) in enumerate(output_polygons.loc[output_polygons.polygon_description.isin(["POI"])].iterrows()):
    
    print(f"{i} ({poly_ind})")
    
    pol_relevant_user_table = align_user_table(polygon, user_table)
    
    to_upload = place_of_origin.get_place_of_origin(polygon, pol_relevant_user_table, project_dataset, client)
    
    if to_upload is not None:
        to_upload_df = pd.concat([to_upload_df, to_upload[(to_upload.count_people != 0)]])
        
    to_upload = place_of_origin.get_place_of_origin(polygon, pol_relevant_user_table, project_dataset, client, hourly = False)
    
    if to_upload is not None:
        to_upload_df_daily = pd.concat([to_upload_df_daily, to_upload[(to_upload.count_people != 0)]])


upload_table_bq(to_upload_df, output_table_hourly, schema = {}, client = client)
upload_table_bq(to_upload_df_daily, output_table_daily, schema = {}, client = client)
