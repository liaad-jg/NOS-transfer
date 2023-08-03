import utils.config as config
from utils.bigquery_queries import finished_trajectories_query, unfinished_trajectories_query
from segment_trajectories import process_finished_trajectories
from utils.bigquery_utils import upload_table_bq


def insert_trajectories(day, time_start, time_end, raw_data_table, project_dataset, bq_client):
    
    finished_trajectories_q = finished_trajectories_query(day, time_start, time_end, raw_data_table, project_dataset)
    unfinished_trajectories_q = unfinished_trajectories_query(day, time_start, time_end, raw_data_table, project_dataset)
    
    finished_trajectories = bq_client.query(finished_trajectories_q).to_dataframe()
    bq_client.query(unfinished_trajectories_q).result()
    
    processed_finished_trajectories, trajectory_summary = process_finished_trajectories(finished_trajectories)
    
    upload_table_bq(trajectory_summary, f"{project_dataset}.{config.table_trajectories}", 
                    schema = {"start_point": "geography",'end_point': "geography"}, client = bq_client)
    
    upload_table_bq(processed_finished_trajectories, f"{project_dataset}.{config.table_trajectory_details}", 
                    schema = {"geography": "geography",'next_geography': "geography"}, client = bq_client)
    
    return processed_finished_trajectories
