import google.auth
from google.cloud import bigquery
import pandas as pd
import utils.config as config
from utils.utils import align_user_table
from utils.bigquery_utils import upload_table_bq
from use_cases import count_people_in_polygon

credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe()

user_table = client.query(f"SELECT * FROM {project_dataset}.{config.table_users}").to_dataframe()

output_table_hourly = f"{output_dataset}.area_count_hourly"
output_table_daily = f"{output_dataset}.area_count_daily"

to_upload_df = pd.DataFrame(columns = ["polygon_id", "datetime", "residency_status", "count_people"])
to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "residency_status", "count_people"])

for i, (poly_ind, polygon) in enumerate(output_polygons.loc[output_polygons.polygon_description.isin(["Concelho", 
                                                                                                      "Freguesia", 
                                                                                                      "POI", 
                                                                                                      "Secção", 
                                                                                                      "Subsecção"])].iterrows()):
    
    print(i, poly_ind)
    
    if i%1001 == 1000:
        upload_table_bq(to_upload_df, output_table_hourly, schema = {"polygon_id":"string"}, client = client)
        upload_table_bq(to_upload_df_daily, output_table_daily, schema = {"polygon_id":"string"}, client = client)
        to_upload_df = pd.DataFrame(columns = ["polygon_id", "datetime", "residency_status", "count_people"])
        to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "residency_status", "count_people"])

    pol_relevant_user_table = align_user_table(polygon, user_table)
    
    to_upload = count_people_in_polygon.count_people_in_polygon(polygon, pol_relevant_user_table, project_dataset, client)

    to_upload_df = pd.concat([to_upload_df, to_upload])
        
    to_upload = count_people_in_polygon.count_people_in_polygon(polygon, pol_relevant_user_table, project_dataset, client, hourly = False)
    
    to_upload_df_daily = pd.concat([to_upload_df_daily, to_upload])
    
    
upload_table_bq(to_upload_df, output_table_hourly, schema = {"polygon_id":"string"}, client = client)
upload_table_bq(to_upload_df_daily, output_table_daily, schema = {"polygon_id":"string"}, client = client)
