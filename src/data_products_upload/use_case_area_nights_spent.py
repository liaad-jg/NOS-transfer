# The number of nights spent by people of certain residency status in a polygon_id
import google.auth
from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)

def aggregate_geolevels_in_user_profile(client, source_dataset, user_profile_table, destination_dataset, polygon_table):
    polygons = client.query(f"SELECT * FROM `{destination_dataset}.{polygon_table}`").to_dataframe()
    user_profile = client.query(f"SELECT * FROM `{source_dataset}.{user_profile_table}`").to_dataframe()
    user_profile['day_part'] = user_profile['day_part'].astype(str)
    poi= client.query(f"SELECT distinct polcode, sccode as polygon_id FROM `{destination_dataset}.{polygon_table}` where polygon_description='POI'").to_dataframe()
    print(poi)

    
    user_profile_sccode = user_profile[user_profile['polygon_id'].isin(polygons['sccode'])]
    user_profile_frcode = user_profile[user_profile['polygon_id'].isin(polygons['frcode'])]
    user_profile_cocode = user_profile[user_profile['polygon_id'].isin(polygons['cocode'])]
    user_profile_POI = user_profile_sccode.merge(poi.loc[:, ["polygon_id","polcode"]].drop_duplicates(), on="polygon_id", how="right")
    #user_profile_POI= user_profile_pcode.drop_duplicates(subset=['imsi', 'polcode']).reset_index(drop=True)


    return user_profile_sccode, user_profile_frcode, user_profile_cocode, user_profile_POI


def aggregate_geolevels_in_user_profile1(client, source_dataset, user_profile_table, destination_dataset, polygon_table):
    polygons = client.query(f"SELECT * FROM `{destination_dataset}.{polygon_table}`").to_dataframe()
    poi= client.query(f"SELECT polcode, sccode FROM `{destination_dataset}.{polygon_table}` where polygon_description='POI'").to_dataframe()

    user_profile = client.query(f"SELECT * FROM `{source_dataset}.{user_profile_table}`").to_dataframe()
    user_profile['day_part'] = user_profile['day_part'].astype(str)

    # Define the desired order of categories
    status_order = ['resident','national tourist','international tourist','regular visitor','casual visitor','commuter']

    # Convert the 'status' column to the Categorical data type with the desired category order
    user_profile['residential_status'] = pd.Categorical(user_profile['residential_status'], categories=status_order, ordered=True)

    # Sort the DataFrame based on the 'status' column
    user_profile_sorted = user_profile.sort_values('residential_status')

    # Drop duplicates based on 'user' and 'ccode' columns
    #user_profile_sorted_deduplicated = user_profile_sorted.drop_duplicates(subset=['user', 'ccode'])

    user_profile_sccode = user_profile_sorted.merge(polygons.loc[:, ["sccode", "frcode", "cocode"]].drop_duplicates(), on="sccode", how="left")
    user_profile_frcode= user_profile_sccode.drop_duplicates(subset=['imsi', 'frcode']).reset_index(drop=True)
    user_profile_cocode= user_profile_sccode.drop_duplicates(subset=['imsi', 'cocode']).reset_index(drop=True)
    user_profile_pcode = user_profile_sorted.merge(poi.loc[:, ["sccode", "polcode"]].drop_duplicates(), on="sccode", how="right")
    user_profile_POI= user_profile_pcode.drop_duplicates(subset=['imsi', 'polcode']).reset_index(drop=True)


    return user_profile_sccode, user_profile_frcode, user_profile_cocode, user_profile_POI

def populate_area_nights_spent(client,level,user_profile_table, destination_dataset, destination_table_id, start_date, end_date):
    # Convert DataFrame to temporary BigQuery table
    temp_table_id = 'rw_data_west1.temp_table'
    to_gbq(user_profile_table, temp_table_id, project_id='cityanalyser-inesc-lab-300200', if_exists='replace')
    print('length temp:', len(user_profile_table))

    
    query = f"""
    INSERT INTO `{destination_dataset}.{destination_table_id}` (polygon_id, date_start, date_end, residency_status, number_nights_spent, count_people)
    SELECT {level} AS polygon_id, '{start_date}' AS start_date, '{end_date}' AS end_date, residential_status AS residency_status, CAST(night_count AS INT64) AS number_nights_spent, COUNT(DISTINCT imsi) AS total_count
    FROM `{temp_table_id}`
    WHERE residential_status in ('resident', 'national tourist', 'international tourist') AND day_part BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY polygon_id, residency_status, number_nights_spent
    UNION ALL
    SELECT {level} AS polygon_id, '{start_date}' AS start_date, '{end_date}' AS end_date, residential_status AS residency_status, 0 AS number_nights_spent, COUNT(DISTINCT imsi) AS total_count
    FROM `{temp_table_id}`
    WHERE residential_status in ('commuter', 'casual visitor', 'regular visitor') AND day_part BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY polygon_id, residency_status, number_nights_spent
    """

    # Run the query
    job = client.query(query)
    job.result()  # Wait for the query to complete
    #to_gbq(query, project_id='cityanalyser-inesc-lab-300200')

    print(f"Data populated into {destination_dataset}.{destination_table_id} successfully.")
    
    query = f"""SELECT sum(count_people) from {destination_dataset}.{destination_table_id}"""
    print(client.query(query).to_dataframe())
    
    query = f"""SELECT * from {destination_dataset}.{destination_table_id}"""
    print(client.query(query).to_dataframe())


    
if __name__ == "__main__": 
    user_profile_sccode, user_profile_frcode, user_profile_cocode, user_profile_POI = aggregate_geolevels_in_user_profile(client, 'rw_data_west1', 'user_profile_demo', 'data_products', 'polygons')
    print(user_profile_sccode)
    print(user_profile_POI)
   
    populate_area_nights_spent(client,'polygon_id', user_profile_sccode, 'data_products', 'area_nights_spent', '2023-04-25', '2023-05-01')
    populate_area_nights_spent(client, 'polygon_id', user_profile_frcode, 'data_products', 'area_nights_spent', '2023-04-25', '2023-05-01')
    populate_area_nights_spent(client, 'polygon_id', user_profile_cocode, 'data_products', 'area_nights_spent', '2023-04-25', '2023-05-01')
    populate_area_nights_spent(client, 'polcode', user_profile_POI, 'data_products', 'area_nights_spent', '2023-04-25', '2023-05-01')