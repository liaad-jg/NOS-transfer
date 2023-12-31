# People with certain residency in a polygon staying in a hotel/home/other/undefined
import google.auth
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
from pandas_gbq import to_gbq


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)

def aggregate_geolevels_in_user_profile(client, source_dataset, user_profile_table, destination_dataset, polygon_table):
    polygons = client.query(f"SELECT * FROM `{destination_dataset}.{polygon_table}`").to_dataframe()
    user_profile = client.query(f"SELECT * FROM `{source_dataset}.{user_profile_table}`").to_dataframe()
    user_profile['day_part'] = user_profile['day_part'].astype(str)
    poi= client.query(f"SELECT distinct polcode, sccode as polygon_id FROM `{destination_dataset}.{polygon_table}` where polygon_description='POI'").to_dataframe()
    
    user_profile_sccode = user_profile[user_profile['polygon_id'].isin(polygons['sccode'])]
    user_profile_frcode = user_profile[user_profile['polygon_id'].isin(polygons['frcode'])]
    user_profile_cocode = user_profile[user_profile['polygon_id'].isin(polygons['cocode'])]
    user_profile_POI = user_profile_sccode.merge(poi.loc[:, ["polygon_id","polcode"]].drop_duplicates(), on="polygon_id", how="right")
    #user_profile_POI= user_profile_pcode.drop_duplicates(subset=['imsi', 'polcode']).reset_index(drop=True)


    return user_profile_sccode, user_profile_frcode, user_profile_cocode, user_profile_POI


def populate_night_stay_type(client, level, source_dataset, destination_dataset, user_profile_table, source_table_id, destination_table_id, start_date, end_date):
    # Convert DataFrame to temporary BigQuery table
    temp_table_id = 'rw_data_west1.temp_table_night'
    pandas_gbq.to_gbq(user_profile_table, temp_table_id, if_exists='replace')

 # Prepare SQL query to populate the destination table
    q= f"""WITH user_status AS (
    SELECT '{start_date}' as date_start,'{end_date}' as date_end, {level} as polygon_id, residential_status as residency_status, imsi
    FROM `{temp_table_id}`
    WHERE day_part BETWEEN '{start_date}' AND '{end_date}'
    ),
    user_night_status AS (
    SELECT imsi, night_stay_establishment
    FROM `{source_dataset}.{source_table_id}`
    WHERE day_part BETWEEN '{start_date}' AND '{end_date}'
    ),
    result AS (
    SELECT 
    us.date_start,
    us.date_end,
    us.polygon_id,
    us.residency_status,
    ups.night_stay_establishment,
    COUNT(DISTINCT us.imsi) AS count_people
    FROM user_status us
    LEFT JOIN user_night_status ups ON us.imsi = ups.imsi
    GROUP BY us.date_start, us.date_end, us.polygon_id, us.residency_status, ups.night_stay_establishment
    )
    SELECT polygon_id, date_start, date_end, residency_status, night_stay_establishment, count_people FROM result"""
    d=client.query(q).to_dataframe()
    pandas_gbq.to_gbq(d, f"{destination_dataset}.{destination_table_id}", if_exists='append')
    
    
if __name__ == "__main__": 
    user_profile_sccode, user_profile_frcode, user_profile_cocode, user_profile_POI = aggregate_geolevels_in_user_profile(client, 'rw_data_west1', 'user_profile_demo', 'data_products', 'polygons')
    
    populate_night_stay_type(client,'polygon_id', 'rw_data_west1', 'data_products', user_profile_sccode, 'night_stay_type_demo','night_stay_type', '2023-04-25','2023-05-01')
    populate_night_stay_type(client,'polygon_id', 'rw_data_west1', 'data_products', user_profile_frcode, 'night_stay_type_demo','night_stay_type', '2023-04-25','2023-05-01')
    populate_night_stay_type(client,'polygon_id', 'rw_data_west1', 'data_products', user_profile_cocode, 'night_stay_type_demo','night_stay_type', '2023-04-25', '2023-05-01')
    populate_night_stay_type(client, 'polcode', 'rw_data_west1', 'data_products', user_profile_POI, 'night_stay_type_demo','night_stay_type', '2023-04-25', '2023-05-01')