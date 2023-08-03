import google.auth
from google.cloud import bigquery
import pandas as pd
import utils.config as config
from utils.utils import align_user_table
from utils.bigquery_utils import upload_table_bq
from use_cases.catchment_area import catchment_area, catchment_area_daily

credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"

bgri_table = client.query(f"""SELECT DISTINCT CAST(dt21 AS STRING) as dicode, 
                                              CAST(dtmn21 AS STRING) as cocode 
                              FROM dsmanalytics-p-032502.reference_ine.tb_ine_bgri_2021_portugal_shape""").to_dataframe()
output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe().merge(bgri_table, on="cocode")

output_table_hourly = f"{output_dataset}.catchment_area_hourly"
output_table_daily = f"{output_dataset}.catchment_area_daily"

user_table = client.query(f"SELECT * FROM {project_dataset}.{config.table_users}").to_dataframe()
user_table = user_table.merge(output_polygons.loc[output_polygons.polygon_description.isin(["Secção",
                                                                                            "Concelho",
                                                                                            "Freguesia"]), 
                                                  ["polygon_id", "cocode", "dicode"]].drop_duplicates(), on="polygon_id")


to_upload_df_hourly = pd.DataFrame(columns = ["polygon_id", "datetime", "c_area", "count_people"])
to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "c_area", "count_people"])

for i,(poly_ind, polygon) in enumerate(output_polygons.loc[output_polygons.polygon_description.isin(["Concelho", 
                                                                                                      "Freguesia", 
                                                                                                      "Subsecção", 
                                                                                                      "Secção", 
                                                                                                      "POI"])].iterrows()):
    
    print(f"{i} ({poly_ind})")
    
    if i%501 == 500:
        upload_table_bq(to_upload_df_hourly, output_table_hourly, {}, client)
        to_upload_df_hourly = pd.DataFrame(columns = ["polygon_id", "datetime", "c_area", "count_people"])
        upload_table_bq(to_upload_df_daily, output_table_daily, {}, client)
        to_upload_df_daily = pd.DataFrame(columns = ["polygon_id", "date", "c_area", "count_people"])


    c_area = catchment_area(polygon, user_table, project_dataset, client)
    to_upload_df_hourly = pd.concat([to_upload_df_hourly, c_area])
    c_area = catchment_area_daily(polygon, user_table, project_dataset, client)
    to_upload_df_daily = pd.concat([to_upload_df_daily, c_area])
          
upload_table_bq(to_upload_df_hourly, output_table_hourly, {}, client)
upload_table_bq(to_upload_df_daily, output_table_daily, {}, client)
