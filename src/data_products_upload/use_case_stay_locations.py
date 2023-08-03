#In a given time period (days) number of users working/studying (staying) in a polygon_id and their residency status in that polygon
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
    
    user_profile_sccode = user_profile[user_profile['polygon_id'].isin(polygons['sccode'])]
    user_profile_frcode = user_profile[user_profile['polygon_id'].isin(polygons['frcode'])]
    user_profile_cocode = user_profile[user_profile['polygon_id'].isin(polygons['cocode'])]

    return user_profile_sccode, user_profile_frcode, user_profile_cocode


def aggregate_geolevels_in_user_profile1(client, source_dataset, user_profile_table, destination_dataset, polygon_table):
    polygons = client.query(f"SELECT * FROM `{destination_dataset}.{polygon_table}`").to_dataframe()
    #user_profile = client.query(f"SELECT * FROM `{source_dataset}.{user_profile_table}`").to_dataframe()
    user_profile = client.query(f"SELECT imsi, polygon_id as sccode, residential_status, day_part FROM `{source_dataset}.{user_profile_table}` where polygon_id in (select distinct(sccode) from data_products.polygons where polygon_description = 'Secção')").to_dataframe()
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

    return user_profile_sccode, user_profile_frcode, user_profile_cocode

def aggregate_geolevels_in_work_place(client, source_dataset, work_place_table, destination_dataset, polygon_table):
    polygons = client.query(f"SELECT * FROM `{destination_dataset}.{polygon_table}`").to_dataframe()
    work_place = client.query(f"SELECT DISTINCT imsi, s2code, day_part FROM `{source_dataset}.{work_place_table}` WHERE work_place = 'yes'").to_dataframe()
    work_place['day_part'] = work_place['day_part'].astype(str)
    work_place['s2code'] = work_place['s2code'].astype(str)

    
    # Drop duplicates based on 'user' and 'ccode' columns
    #work_place_sorted_deduplicated = work_place_sorted.drop_duplicates(subset=['user', 'ccode'])

    work_place_s2code = work_place.merge(polygons.loc[:, ["s2code", "sscode","sccode", "frcode", "cocode"]].drop_duplicates(), on="s2code", how="left")
    work_place_sscode= work_place_s2code.drop_duplicates(subset=['imsi', 'sscode']).reset_index(drop=True)
    work_place_sccode= work_place_s2code.drop_duplicates(subset=['imsi', 'sccode']).reset_index(drop=True)
    work_place_frcode= work_place_s2code.drop_duplicates(subset=['imsi', 'frcode']).reset_index(drop=True)
    work_place_cocode= work_place_s2code.drop_duplicates(subset=['imsi', 'cocode']).reset_index(drop=True)

    return work_place_s2code,work_place_sscode,work_place_sccode, work_place_frcode, work_place_cocode
    
    
def populate_stay_location(client, level1, level2, user_profile_table, work_place_table, source_dataset, source_table_id, destination_dataset, destination_table_id, date_start, date_end, polygons_table):
    # Convert DataFrame to temporary BigQuery table
    temp_table_user = 'rw_data_west1.temp_table_user'
    to_gbq(user_profile_table, temp_table_user, project_id='cityanalyser-inesc-lab-300200', if_exists='replace')
    print('length temp:', len(user_profile_table))
    
    #query = f"""SELECT count(distinct(imsi)) from {temp_table_id} where residential_status = 'resident'"""
    #print(client.query(query).to_dataframe())
    temp_table_work = 'rw_data_west1.temp_table_work'
    to_gbq(work_place_table, temp_table_work, project_id='cityanalyser-inesc-lab-300200', if_exists='replace')
    print('length temp:', len(work_place_table))
    
    

 # Prepare SQL query to populate the destination table
    query = f"""
    WITH workplace_data AS (
        SELECT DISTINCT p.imsi, wp.{level1} as polygon_id, wp.{level2} as polygon_id2,  p.professional_status,
        FROM `{temp_table_work}` wp
        JOIN `{source_dataset}.{source_table_id}` p ON p.imsi = wp.imsi
        WHERE wp.day_part BETWEEN '{date_start}' AND '{date_end}'
    ),
    userprofile_data AS (
        SELECT DISTINCT up.imsi, up.residential_status as residency_status, up.polygon_id
        FROM {temp_table_user} up
        WHERE up.day_part BETWEEN '{date_start}' AND '{date_end}'
    ),
    output_data AS (
        SELECT '{date_start}' AS date_start, '{date_end}' AS date_end, wp.polygon_id, wp.professional_status, up.residency_status, COUNT(DISTINCT wp.imsi) AS count_people
        FROM workplace_data wp
        JOIN userprofile_data up ON wp.imsi = up.imsi and wp.polygon_id2 = up.polygon_id
        GROUP BY wp.polygon_id, wp.professional_status, up.residency_status
    )
    SELECT date_start, date_end, od.polygon_id, od.professional_status, od.residency_status, od.count_people
    FROM output_data od
    """


    # Write the output to the specified table

    df=client.query(query).to_dataframe()
    #print(df)
    pandas_gbq.to_gbq(df, f"{destination_dataset}.{destination_table_id}", if_exists='append')
    
    
if __name__ == "__main__": 
    user_profile_sccode, user_profile_frcode, user_profile_cocode = aggregate_geolevels_in_user_profile(client, 'rw_data_west1', 'user_profile_demo', 'data_products', 'polygons')
        
    work_place_s2code, work_place_sscode, work_place_sccode, work_place_frcode, work_place_cocode = aggregate_geolevels_in_work_place(client, 'rw_data_west1','work_place_s2cell_demo', 'data_products', 'polygons')
    
    populate_stay_location(client,'s2code','sccode', user_profile_sccode, work_place_s2code, 'rw_data_west1','user_professional_status_profile_demo','data_products', 'stay_locations', '2023-04-25', '2023-05-01', 'polygons')
    
    populate_stay_location(client,'sscode','sccode', user_profile_sccode, work_place_sscode, 'rw_data_west1','user_professional_status_profile_demo','data_products', 'stay_locations', '2023-04-25', '2023-05-01', 'polygons')
    
    populate_stay_location(client, 'sccode', 'sccode', user_profile_sccode, work_place_sccode, 'rw_data_west1','user_professional_status_profile_demo', 'data_products', 'stay_loacations', '2023-04-25', '2023-05-01', 'polygons')
    
    populate_stay_location(client, 'frcode', 'frcode', user_profile_frcode, work_place_frcode, 'rw_data_west1','user_professional_status_profile_demo', 'data_products', 'stay_locations', '2023-04-25', '2023-05-01', 'polygons')
    
    populate_stay_location(client, 'cocode', 'cocode', user_profile_cocode, work_place_cocode, 'rw_data_west1','user_professional_status_profile_demo', 'data_products', 'stay_locations', '2023-04-25', '2023-05-01', 'polygons')

    
 