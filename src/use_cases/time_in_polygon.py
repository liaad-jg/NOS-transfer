import pandas as pd
import utils.config as config
import numpy as np
from building_blocks.BB1 import BB1_as_details, BB1_stay_points
import shapely


def time_in_area_query(day, polygon, project_dataset):
    
    query = f"""WITH visits AS (
                    SELECT 
                        {config.USER_ID},
                        datetime,
                        ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), geography) AS inside_polygon
                    FROM {project_dataset}.{config.table_trajectory_details}
                    WHERE DATE(datetime) = DATE('{day}')
                ),
                visits_with_change AS (
                    SELECT
                        {config.USER_ID},
                        datetime,
                        inside_polygon,
                        LAG(inside_polygon) OVER (PARTITION BY {config.USER_ID} ORDER BY datetime ASC) AS prev_inside_polygon
                    FROM visits
                ),
                visit_start AS (
                    SELECT
                        {config.USER_ID},
                        datetime AS time_in,
                        inside_polygon,
                        ROW_NUMBER() OVER (PARTITION BY {config.USER_ID} ORDER BY datetime ASC) AS visit_num
                    FROM visits_with_change
                    WHERE inside_polygon AND (prev_inside_polygon IS NULL OR NOT prev_inside_polygon)
                ),
                visit_end AS (
                    SELECT
                        {config.USER_ID},
                        datetime AS time_out,
                        inside_polygon,
                        ROW_NUMBER() OVER (PARTITION BY {config.USER_ID} ORDER BY datetime ASC) AS visit_num
                    FROM visits_with_change
                    WHERE NOT inside_polygon AND prev_inside_polygon
                )
                SELECT
                    v.{config.USER_ID},
                    v.time_in,
                    ve.time_out
                FROM visit_start AS v JOIN visit_end AS ve 
                     ON v.{config.USER_ID} = ve.{config.USER_ID} AND v.visit_num = ve.visit_num
                ORDER BY v.{config.USER_ID}, v.time_in
                """
    return query


def time_spent_in_area(day, polygon, project_dataset, bq_client, hourly = True):
    
    time_in_area_day = bq_client.query(time_in_area_query(day, polygon, project_dataset)).to_dataframe()
    
    if hourly:
        return_df = pd.DataFrame(columns = [f"{config.USER_ID}", "datetime", "stay_time"])
        for hour in range(24):
            time_start = pd.to_datetime(f"{day.date()} {hour:02d}:00:00", utc = True)
            time_end = time_start + pd.Timedelta(hours=1)
            time_in_area_hour = time_in_area_day[(time_in_area_day.time_in < time_end) & (time_in_area_day.time_out > time_start)].copy()
            if len(time_in_area_hour) == 0:
                continue
            time_in_area_hour["new_time_in"] = np.fmax(pd.to_datetime(time_start, utc=True), time_in_area_hour["time_in"])
            time_in_area_hour["new_time_out"] = np.fmin(pd.to_datetime(time_end, utc=True), time_in_area_hour["time_out"])
            time_in_area_hour["new_stay_time"] = (time_in_area_hour["new_time_out"] - time_in_area_hour["new_time_in"]).dt.total_seconds()
            time_in_area_hour = time_in_area_hour.groupby(config.USER_ID).agg(stay_time = ("new_stay_time", "sum")).reset_index()
            time_in_area_hour["datetime"] = time_start
            return_df = pd.concat([return_df, time_in_area_hour])
    
    else:
        time_in_area_day["stay_time"] = (time_in_area_day["time_out"] - time_in_area_day["time_in"]).dt.total_seconds()
        return_df = time_in_area_day.groupby(config.USER_ID).agg(stay_time = ("stay_time", "sum")).reset_index()
    
    return return_df


def time_in_area_wrapper(row, day, project_dataset, bq_client):
    
    user_stay_time = time_spent_in_area(day, shapely.wkt.loads(row["geography"]), project_dataset, bq_client, hourly=True)
    
    user_stay_time["g_interest"] = row["g_interest"]
    user_stay_time["grupo"] = row["Grupo"]
    user_stay_time["sccode"] = row["sccode"] 
    user_stay_time["frcode"] = row["frcode"] 
    user_stay_time["cocode"] = row["cocode"]
    
    return user_stay_time
