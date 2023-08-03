import google.auth
from google.cloud import bigquery
import pandas as pd
import utils.config as config
from utils.utils import align_user_table
from utils.bigquery_utils import upload_table_bq
from use_cases.time_in_polygon import time_in_area_wrapper
from use_cases.share_of_time import share_of_time


def split_sonae(grupo):
    if grupo == "Sonae":
        return grupo
    return "Not Sonae"


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_table = f"{output_dataset}.area_share_of_time"

output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe()

user_table = client.query(f"SELECT * FROM {project_dataset}.{config.table_users}").to_dataframe()

our_supermarkets = f"{project_dataset}.supermarkets_table"
supermarkets_df = client.query(f"SELECT * FROM {our_supermarkets}").to_dataframe()

supermarkets_df["g_interest"] = supermarkets_df["Grupo"].apply(lambda x: split_sonae(x))
seccoes = supermarkets_df.sccode.unique()
seccoes_output_poly = output_polygons.loc[(output_polygons.sccode.isin(seccoes)) & (output_polygons.polygon_description == 'Secção'), :]
fregs = supermarkets_df.frcode.unique()
fregs_output_poly = output_polygons.loc[(output_polygons.frcode.isin(fregs)) & (output_polygons.polygon_description == 'Freguesia'), :]
concs = supermarkets_df.cocode.unique()
concs_output_poly = output_polygons.loc[(output_polygons.cocode.isin(concs)) & (output_polygons.polygon_description == 'Concelho'), :]

current_datetime = pd.Timestamp(config.start_datetime_demo)
timedelta = pd.Timedelta(days=1)
max_time = pd.Timestamp(config.end_datetime_demo)

while current_datetime < max_time:
    
    share_of_time_df = pd.DataFrame(columns = ["polygon_id", "datetime", "residency_status", "grupo", "seconds_in_store", "share_of_time_percentage"])
    time_in_super = pd.concat(list(supermarkets_df.apply(lambda x: time_in_area_wrapper(x, 
                                                                                        current_datetime, 
                                                                                        project_dataset, 
                                                                                        client), axis=1)))
    
    time_in_super["g_interest"] = pd.Categorical(time_in_super["g_interest"], categories=["Sonae", "Not Sonae"])
  
    for _, poly in seccoes_output_poly.iterrows():
        
        sups_of_interest = time_in_super.loc[time_in_super.sccode == poly["sccode"], :]
        poly_user_table = align_user_table(poly, user_table)
        sot = share_of_time(sups_of_interest, poly, poly_user_table, str(current_datetime.date()))
        if sot is not None:
            share_of_time_df = pd.concat([share_of_time_df, sot])
        
    for _, poly in fregs_output_poly.iterrows():
        
        sups_of_interest = time_in_super.loc[time_in_super.frcode == poly["frcode"], :]
        poly_user_table = align_user_table(poly, user_table)
        sot = share_of_time(sups_of_interest, poly, poly_user_table, str(current_datetime.date()))
        if sot is not None:
            share_of_time_df = pd.concat([share_of_time_df, sot])
        
    for _, poly in concs_output_poly.iterrows():
        
        sups_of_interest = time_in_super.loc[time_in_super.cocode == poly["cocode"], :]
        poly_user_table = align_user_table(poly, user_table)
        sot = share_of_time(sups_of_interest, poly, poly_user_table, str(current_datetime.date()))
        if sot is not None:
            share_of_time_df = pd.concat([share_of_time_df, sot])
        
    share_of_time_df.fillna(0, inplace=True)
    upload_table_bq(share_of_time_df, output_table, schema = {}, client = client)
    
    current_datetime = current_datetime + timedelta