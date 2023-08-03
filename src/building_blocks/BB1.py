import utils.config as config
from utils.utils import get_reverse_polygon, check_coordinates_order

"""
BB1 - Trajectories crossing polygons
Calculate trajectories that cross the geo polygon for the given time interval

Params:
polygon - A polygon that defines the area of interest
start_datetime, end_datetime - Time interval  for the trajectories that cross the polygon

Output:
Trajectories that cross the given polygon in the given time interval grouped by user
"""

def get_trajectory_details(trajectories_list, project, bigquery_client):
    
    #TO DO: if list of trajectories very big, UNNEST makes query too long
    
    query = f"""SELECT TRAJ_DET.*
    FROM {project}.{config.table_trajectory_details} AS TRAJ_DET
    WHERE TRAJ_DET.trajectory_id IN UNNEST({trajectories_list})
    ORDER BY TRAJ_DET.{config.USER_ID}"""
   
    results = bigquery_client.query(query).to_dataframe()
    
    return results


def BB1_simple(start_datetime, end_datetime, polygon, project, bigquery_client): 
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
    
    query = f"""SELECT TRAJ.trajectory_id, TRAJ.{config.USER_ID} 
    FROM {project}.{config.table_trajectories} AS TRAJ 
    INNER JOIN {project}.{config.table_trajectory_details} AS TRAJ_DET 
    ON TRAJ.trajectory_id = TRAJ_DET.trajectory_id 
    WHERE TRAJ_DET.datetime BETWEEN '{start_datetime}' AND '{end_datetime}' 
    AND ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), TRAJ_DET.geography) 
    GROUP BY TRAJ.trajectory_id, TRAJ.{config.USER_ID} 
    ORDER BY TRAJ.{config.USER_ID}"""   
  
    results = bigquery_client.query(query).to_dataframe()
    
    return results


def BB1_as_details(start_datetime, end_datetime, polygon, project_dataset, bigquery_client): 
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
    
    query = f"""SELECT TRAJ_DET.*, 
                ST_DISTANCE(TRAJ_DET.previous_point, TRAJ_DET.geography) as previous_distance, 
                ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), TRAJ_DET.geography) as intersect_flag
    FROM (SELECT *, LAG(geography) OVER (PARTITION BY {config.USER_ID} ORDER BY datetime ASC) AS previous_point
          FROM {project_dataset}.{config.table_trajectory_details} 
          WHERE trajectory_id IN (SELECT temp_traj.trajectory_id 
                                FROM {project_dataset}.{config.table_trajectory_details} as temp_traj
                                WHERE temp_traj.datetime BETWEEN '{start_datetime}' AND '{end_datetime}' 
                                    AND ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), temp_traj.geography))) AS TRAJ_DET
    ORDER BY TRAJ_DET.{config.USER_ID}"""   
  
    results = bigquery_client.query(query).to_dataframe()
    
    return results



def BB1_start_end(is_start, start_datetime, end_datetime, polygon, project, bigquery_client):
    
    geography = ''
    if is_start:
        geography = 'start_point'
    else:
        geography = 'end_point'
        
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
    
    query = f"""SELECT trajectory_id, {config.USER_ID} 
    FROM {project}.{config.table_trajectories} 
    WHERE start_time >= '{start_datetime}' AND end_time <= '{end_datetime}' 
    AND ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), {geography}) 
    GROUP BY trajectory_id, {config.USER_ID} 
    ORDER BY {config.USER_ID}"""
   
    results = bigquery_client.query(query).to_dataframe()
    
    return results


def BB1_stay_points(start_datetime, end_datetime, polygon, project, bigquery_client):   
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
    
    query = f"""SELECT * 
    FROM {project}.{config.table_stay_points} 
    WHERE ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), centroid_geography) AND
          ((datetime_arrive BETWEEN '{start_datetime}' AND '{end_datetime}') OR 
           (datetime_leave BETWEEN '{start_datetime}' AND '{end_datetime}') OR
           (datetime_arrive <= '{start_datetime}' AND datetime_leave >= '{end_datetime}'))
    ORDER BY {config.USER_ID}"""
   
    results = bigquery_client.query(query).to_dataframe()
    
    return results


def BB1_by_datetime(polygon, project, bigquery_client):
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
        
    query = f"""SELECT DISTINCT DATE(datetime) as day, 
                EXTRACT(HOUR FROM datetime) as hour, 
                {config.USER_ID}, 
                trajectory_id  
    FROM {project}.{config.table_trajectory_details}
    WHERE ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), geography) AND
          datetime < '{config.end_datetime_demo}'"""   
  
    results = bigquery_client.query(query).to_dataframe()
    
    return results


def BB1_by_day(polygon, project, bigquery_client):
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
        
    query = f"""SELECT DISTINCT DATE(datetime) as date, {config.USER_ID}, trajectory_id
    FROM {project}.{config.table_trajectory_details}
    WHERE ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), geography) AND
          datetime < '{config.end_datetime_demo}'"""   
  
    results = bigquery_client.query(query).to_dataframe()
    
    return results
