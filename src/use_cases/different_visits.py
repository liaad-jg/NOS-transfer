import utils.config as config
import pandas as pd

def get_different_visits_query(polygon, project_dataset):
    
    query = f"""WITH visits AS (
                    SELECT 
                        {config.USER_ID},
                        datetime,
                        ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), geography) AS inside_polygon
                    FROM {project_dataset}.{config.table_trajectory_details}
                    WHERE datetime <= '{config.end_datetime_demo}'
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
                ),
                time_in_polygon AS (
                    SELECT
                        v.{config.USER_ID},
                        v.time_in,
                        ve.time_out
                    FROM visit_start AS v JOIN visit_end AS ve 
                         ON v.{config.USER_ID} = ve.{config.USER_ID} AND v.visit_num = ve.visit_num
                    ORDER BY v.{config.USER_ID}, v.time_in
                ),
                time_outside_polygon AS (
                    SELECT
                        *,
                        TIMESTAMP_DIFF(time_out, time_in, SECOND) as time_inside_polygon,
                        LAG(time_out) OVER (PARTITION BY {config.USER_ID} ORDER BY time_in ASC) as prev_time_out
                    FROM time_in_polygon
                )
                SELECT {config.USER_ID}, time_in, time_out
                FROM time_outside_polygon
                WHERE time_inside_polygon >= {config.THRESHOLD_VISIT_INSIDE} AND
                      (prev_time_out IS NULL OR TIMESTAMP_DIFF(time_in, prev_time_out, SECOND) >= {config.THRESHOLD_VISIT_OUTSIDE})
                """
    
    return query
    
    
def get_always_inside_query(polygon, day_start, day_end, project_dataset):
    
    query_always_inside = f"""SELECT {config.USER_ID}, 1 as number_visits
                          FROM {project_dataset}.{config.table_trajectory_details}
                          WHERE day_part >= DATE('{day_start}') AND day_part < DATE('{day_end}')
                          GROUP BY {config.USER_ID}
                          HAVING COUNT(*) = SUM(CASE WHEN ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), geography) THEN 1 ELSE 0 END)"""
    
    return query_always_inside



def get_dist_values(diff_visits_df):
    
    number_different_visits = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1000]  #  1000 is just a randomly big number for the open interval 10+
    ii =  pd.IntervalIndex.from_tuples([(number_different_visits[i], number_different_visits[i+1]) for i in range(len(number_different_visits)-1)],closed="right")
    
    different_visits_dist = diff_visits_df.groupby(pd.cut(diff_visits_df.number_visits.values,ii)).size().values
    labels = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10+"]
    
    return pd.DataFrame.from_dict(dict(number_different_visits = labels, count_people = different_visits_dist))


def get_different_visits_dist(polygon, diff_visits_df, day_start, day_end, user_table, project_dataset, bq_client):
    
    always_inside_query = get_always_inside_query(polygon, day_start, day_end, project_dataset)
    
    users_always_inside = bq_client.query(always_inside_query).to_dataframe()
    
    diff_visits_dist = diff_visits_df.groupby(config.USER_ID).size().reset_index(name="number_visits").sort_values("number_visits")
    all_users_diff_visits_dist = pd.concat([diff_visits_dist, users_always_inside])
    
    total_diff_visits = get_dist_values(all_users_diff_visits_dist)
    total_diff_visits["residential_status"] = "total"
    
    diff_visits_with_labels = all_users_diff_visits_dist.merge(user_table, on=config.USER_ID, how="left")
    diff_visits = diff_visits_with_labels.groupby("residential_status").apply(lambda x: get_dist_values(x)).reset_index()
    
    return pd.concat([diff_visits.loc[:, ["number_different_visits", "count_people", "residential_status"]], 
                      total_diff_visits])
