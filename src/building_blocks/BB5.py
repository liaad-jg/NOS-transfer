import pandas as pd
from scipy.stats import zscore
from utils.utils import list_polygon_to_s2list, list_polygon_to_section, list_polygon_to_freguesia, split_dates
import numpy as np
from utils import config


def get_spatial_significance(df):
    df["spatial_significance"] = zscore(df.count_people.values)
    return df


def get_temporal_significance(df):
    df["temporal_significance"] = zscore(df.count_people.values)
    return df


def large_traffic_flow(area_polygon, start_time, end_time, bq_client, project_dataset, **kwargs):
    
    if kwargs.get("polygon_cover") is None:
        s2_polygon_cover = list_polygon_to_s2list(area_polygon, config.s2_level, bq_client)
    else:
        s2_polygon_cover = kwargs["polygon_cover"]
    
    sql = f"""WITH s2cells AS (
                  SELECT S2_CELLIDFROMPOINT(geography, {config.s2_level}) as s2_id, {config.USER_ID}, 
                  DATE(datetime) as day, EXTRACT(HOUR FROM datetime) as hour
                  FROM {project_dataset}.{config.table_trajectory_details}
                  WHERE datetime BETWEEN '{start_time}' AND '{end_time}'
              )
              SELECT s2_id, day, hour, COUNT(DISTINCT {config.USER_ID}) as unique_users
              FROM s2cells
              GROUP BY s2_id, day, hour"""
    
    people_per_cell = bq_client.query(sql).to_dataframe()
    days_hours = split_dates(start_time, end_time, granularity = {"hours": 1})
    polygon_cover_df = pd.DataFrame(np.array([[int(s2_id), int(day.split("-")[-1]), hour, 0] for s2_id in s2_polygon_cover for (day, hour) in days_hours]), 
                                    columns = ["s2_id", "day", "hour", "default_number"], dtype=int)
    polygon_cover_df["day"] = [pd.Timestamp(day).date() for _ in s2_polygon_cover for (day, _) in days_hours]
    joined_counts = polygon_cover_df.merge(people_per_cell, how="left")
    joined_counts["count_people"] = np.fmax(joined_counts.unique_users, joined_counts.default_number)
    joined_counts = joined_counts.groupby(["day", "hour"]).apply(get_spatial_significance)
    joined_counts = joined_counts.groupby("s2_id").apply(get_temporal_significance)
    return joined_counts.loc[:, ["s2_id", "day", "hour", "count_people", "spatial_significance", "temporal_significance"]]
    

def large_traffic_flow_section(area_polygon, start_time, end_time, bq_client, project_dataset, **kwargs):
    
    if kwargs.get("polygon_cover") is None:
        section_polygon_cover = list_polygon_to_section(area_polygon, bq_client, project_dataset)
    else:
        section_polygon_cover = kwargs["polygon_cover"]
    
    sql = f"""WITH s2cells AS (
                  SELECT CAST(S2_CELLIDFROMPOINT(geography, {config.s2_level}) AS STRING) AS s2_id, {config.USER_ID}, 
                  DATE(datetime) as day, EXTRACT(HOUR FROM datetime) as hour
                  FROM {project_dataset}.{config.table_trajectory_details}
                  WHERE datetime BETWEEN '{start_time}' AND '{end_time}'
              ),
              scc_mapper AS (
                  SELECT DISTINCT s2code, sccode
                  FROM {project_dataset}.{config.table_polygons}
                  WHERE polygon_description = 'Celula'
              )
              SELECT CAST(scc_mapper.sccode AS STRING) AS sccode, s2cells.day, s2cells.hour, COUNT(DISTINCT s2cells.{config.USER_ID}) as unique_users
              FROM s2cells INNER JOIN scc_mapper ON s2cells.s2_id = scc_mapper.s2code
              GROUP BY scc_mapper.sccode, s2cells.day, s2cells.hour"""
    
    people_per_section = bq_client.query(sql).to_dataframe()
    days_hours = split_dates(start_time, end_time, granularity = {"hours": 1})
    polygon_cover_df = pd.DataFrame(np.array([[sccode, day.split("-"), hour, 0] for sccode in section_polygon_cover for (day, hour) in days_hours], dtype=object), 
                                    columns = ["sccode", "day", "hour", "default_number"])
    polygon_cover_df.default_number = polygon_cover_df.default_number.astype(int)
    polygon_cover_df["day"] = [pd.Timestamp(day).date() for _ in section_polygon_cover for (day, _) in days_hours]
    joined_counts = polygon_cover_df.merge(people_per_section, how="left")
    joined_counts["count_people"] = np.fmax(joined_counts.unique_users, joined_counts.default_number)
    joined_counts.count_people = joined_counts.count_people.astype(int)
    joined_counts = joined_counts.groupby(["day", "hour"]).apply(get_spatial_significance)
    joined_counts = joined_counts.groupby("sccode").apply(get_temporal_significance)
    return joined_counts.loc[:, ["sccode", "day", "hour", "count_people", "spatial_significance", "temporal_significance"]]
        
    
def large_traffic_flow_freguesia(area_polygon, start_time, end_time, bq_client, project_dataset, **kwargs):
    
    if kwargs.get("polygon_cover") is None:
        freguesia_polygon_cover = list_polygon_to_freguesia(area_polygon, bq_client, project_dataset)
    else:
        freguesia_polygon_cover = kwargs["polygon_cover"]
    
    sql = f"""WITH s2cells AS (
                  SELECT CAST(S2_CELLIDFROMPOINT(geography, {config.s2_level}) AS STRING) AS s2_id, {config.USER_ID}, 
                  DATE(datetime) as day, EXTRACT(HOUR FROM datetime) as hour
                  FROM {project_dataset}.{config.table_trajectory_details}
                  WHERE datetime BETWEEN '{start_time}' AND '{end_time}'
              ),
              frc_mapper AS (
                  SELECT DISTINCT s2code, frcode
                  FROM {project_dataset}.{config.table_polygons}
              )
              SELECT CAST(frc_mapper.frcode AS STRING) AS frcode, s2cells.day, s2cells.hour, COUNT(DISTINCT s2cells.{config.USER_ID}) as unique_users
              FROM s2cells INNER JOIN frc_mapper ON s2cells.s2_id = frc_mapper.s2code
              GROUP BY frc_mapper.frcode, s2cells.day, s2cells.hour"""
    
    people_per_freguesia = bq_client.query(sql).to_dataframe()
    days_hours = split_dates(start_time, end_time, granularity = {"hours": 1})
    polygon_cover_df = pd.DataFrame(np.array([[frcode, day.split("-"), hour, 0] for frcode in freguesia_polygon_cover for (day, hour) in days_hours], dtype=object), 
                                    columns = ["frcode", "day", "hour", "default_number"])
    polygon_cover_df.default_number = polygon_cover_df.default_number.astype(int)
    polygon_cover_df["day"] = [pd.Timestamp(day).date() for _ in freguesia_polygon_cover for (day, _) in days_hours]
    joined_counts = polygon_cover_df.merge(people_per_freguesia, how="left")
    joined_counts["count_people"] = np.fmax(joined_counts.unique_users, joined_counts.default_number)
    joined_counts.count_people = joined_counts.count_people.astype(int)
    joined_counts = joined_counts.groupby(["day", "hour"]).apply(get_spatial_significance)
    joined_counts = joined_counts.groupby("frcode").apply(get_temporal_significance)
    return joined_counts.loc[:, ["frcode", "day", "hour", "count_people", "spatial_significance", "temporal_significance"]]

    
def traffic_flow(area_polygon, start_time, end_time, bq_client, project_dataset, **kwargs):
    
    if kwargs.get("polygon_cover") is None:
        s2_polygon_cover = list_polygon_to_s2list(area_polygon, config.s2_level, bq_client)
    else:
        s2_polygon_cover = kwargs["polygon_cover"]
    
    sql = f"""WITH s2cells AS (
                  SELECT S2_CELLIDFROMPOINT(geography, {config.s2_level}) as s2_id, {config.USER_ID} 
                  FROM {project_dataset}.{config.table_trajectory_details}
                  WHERE datetime BETWEEN '{start_time}' AND '{end_time}'
              )
              SELECT s2_id, COUNT(DISTINCT {config.USER_ID}) as unique_users
              FROM s2cells
              GROUP BY s2_id
           """
    
    people_per_cell = bq_client.query(sql).to_dataframe()
    polygon_cover_df = pd.DataFrame(np.array([[s2_id, 0] for s2_id in s2_polygon_cover]), columns = ["s2_id", "default_number"])
    joined_counts = polygon_cover_df.merge(people_per_cell, how="left", on="s2_id")
    joined_counts["number_users"] = np.fmax(joined_counts.unique_users, joined_counts.default_number)
    joined_counts["significance"] = zscore(joined_counts.number_users.values)
    
    return joined_counts.drop(columns=["default_number", "unique_users"])


def temporal_traffic_anomaly(s2cell_id, bq_client, project_dataset):
    
    
    sql = f"""WITH dates AS (
                  SELECT EXTRACT(DAYOFWEEK FROM datetime) as week_day, 
                         EXTRACT(HOUR FROM datetime) as hour,
                         EXTRACT(DAYOFYEAR from datetime) as day, 
                         {config.USER_ID} 
                  FROM {project_dataset}.{config.table_trajectory_details}
                  WHERE S2_CELLIDFROMPOINT(geography, {config.s2_level}) = {s2cell_id}
              )
              SELECT week_day, hour, day, COUNT(DISTINCT {config.USER_ID}) as unique_users
              FROM dates
              GROUP BY week_day, hour, day
           """

    cell_traffic = bq_client.query(sql).to_dataframe()
    week_group = cell_traffic.groupby(["hour", "week_day"]).agg({"unique_users": "mean"})
    
    base_df =  pd.DataFrame(np.array([[day, hour, 0] for day in range(1,8) for hour in range(0, 24)]),
                            columns = ["week_day", "hour", "number"]).set_index(["week_day","hour"])
    joined = base_df.join(week_group).reset_index()
    joined["number_users"] = np.fmax(joined["number"], joined["unique_users"])
    joined["significance"] = zscore(joined.number_users.values)
    return joined.loc[:, ["week_day", "hour", "number_users", "significance"]]
