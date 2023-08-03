import utils.config as config
from calculate_stay_points import calculate_stay_points
from utils.bigquery_utils import upload_table_bq


def insert_stay_points(processed_finished_trajectories, project_dataset, bq_client):
    
    new_SPs = calculate_stay_points(processed_finished_trajectories)
    
    upload_table_bq(new_SPs, f"{project_dataset}.{config.table_stay_points}", 
                    schema = {"centroid_geography": "geography"}, client = bq_client)
    
    return 
