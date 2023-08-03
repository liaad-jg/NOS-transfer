from utils.bigquery_utils import upload_table_bq
import utils.config as config


def BB6_section(time_start, time_end, project_dataset, bq_client):
    
    sql = f"""WITH consecutives AS (
              SELECT {config.USER_ID},
                  trajectory_id,
                  CONCAT(trajectory_id,'_',EXTRACT(HOUR FROM datetime_leave),'_',EXTRACT(DAY FROM datetime_leave)) AS trajectory_leg,
                  datetime_leave as start_time,
                  centroid_geography as start_point,
                  LEAD(centroid_geography) OVER (PARTITION BY {config.USER_ID}, trajectory_id ORDER BY datetime_arrive, datetime_leave ASC) AS end_point,
                  LEAD(datetime_arrive) OVER (PARTITION BY {config.USER_ID}, trajectory_id ORDER BY datetime_arrive, datetime_leave ASC) AS end_time
              FROM {project_dataset}.{config.table_stay_points}
              WHERE datetime_leave >= '{time_start}' AND datetime_leave < '{time_end}'
          ),
          s2cells AS (
              SELECT CAST(S2_CELLIDFROMPOINT(start_point, {config.s2_level}) AS STRING) as start_s2, 
                     CAST(S2_CELLIDFROMPOINT(end_point, {config.s2_level}) AS STRING) as end_s2, 
                     trajectory_id,
                     trajectory_leg,
                     {config.USER_ID},
                     ST_DISTANCE(start_point, end_point) as total_distance,
                     TIMESTAMP_DIFF(end_time, start_time, SECOND) as total_time
              FROM consecutives
            ),
            scc_mapping AS (
                SELECT DISTINCT s2code, sccode
                FROM {project_dataset}.{config.table_polygons}
                WHERE polygon_description = 'Celula'
            ),
            s2cells_mapped AS (
                SELECT s2cells.*, scc_mapping.sccode as start_scc
                FROM s2cells INNER JOIN scc_mapping
                    ON s2cells.start_s2 = scc_mapping.s2code
            ),
            users_mapped AS (
                SELECT s2cells.start_s2,
                       s2cells.end_s2,
                       s2cells.trajectory_id,
                       s2cells.trajectory_leg,
                       s2cells.{config.USER_ID},
                       s2cells.total_distance,
                       s2cells.total_time,
                       users.residential_status as residency_status
                FROM {project_dataset}.{config.table_users} AS users INNER JOIN s2cells_mapped as s2cells
                    ON users.{config.USER_ID} = s2cells.{config.USER_ID} AND users.polygon_id = s2cells.start_scc
            ),
            start_mapped AS (
                SELECT scc_mapping.sccode as start_scc,
                       users_mapped.end_s2,
                       users_mapped.trajectory_id,
                       users_mapped.trajectory_leg,
                       users_mapped.{config.USER_ID},
                       users_mapped.total_distance,
                       users_mapped.total_time,
                       users_mapped.residency_status
                FROM users_mapped INNER JOIN scc_mapping
                    ON users_mapped.start_s2 = scc_mapping.s2code
            ),
            end_mapped AS (
                SELECT start_mapped.start_scc,
                       scc_mapping.sccode as end_scc,
                       start_mapped.trajectory_id,
                       start_mapped.trajectory_leg,
                       start_mapped.{config.USER_ID},
                       start_mapped.total_distance,
                       start_mapped.total_time,
                       start_mapped.residency_status
                FROM start_mapped INNER JOIN scc_mapping
                    ON start_mapped.end_s2 = scc_mapping.s2code
            ),
            prof_status AS (
                SELECT {config.USER_ID}, professional_status
                FROM {project_dataset}.{config.table_user_professional}
            ),
            mode_of_transport AS (
                SELECT * FROM {project_dataset}.{config.table_mode_of_transport}
            ),
            user_home AS (
                SELECT DISTINCT night_loc.{config.USER_ID}, scc_mapping.sccode as home_scc
                FROM {project_dataset}.{config.table_user_night_location} as night_loc INNER JOIN scc_mapping
                     ON CAST(night_loc.s2code AS STRING) = scc_mapping.s2code
                WHERE night_loc.night_stay_place = 'yes'
            ),
            user_work AS (
                SELECT DISTINCT day_loc.{config.USER_ID}, scc_mapping.sccode as work_scc
                FROM {project_dataset}.{config.table_user_work_location} as day_loc INNER JOIN scc_mapping
                     ON CAST(day_loc.s2code AS STRING) = scc_mapping.s2code
                WHERE day_loc.work_place = 'yes'
            )
            SELECT end_mapped.start_scc, 
                   end_mapped.end_scc, 
                   end_mapped.{config.USER_ID}, 
                   end_mapped.trajectory_id,
                   end_mapped.trajectory_leg,
                   end_mapped.total_distance,
                   end_mapped.total_time,
                   end_mapped.residency_status,
                   prof_status.professional_status,
                   mode_of_transport.mode_of_transport as transport_mode,
                   user_home.home_scc,
                   user_work.work_scc
            FROM end_mapped
                 INNER JOIN mode_of_transport ON end_mapped.trajectory_id = mode_of_transport.trajectory_id
                 LEFT JOIN prof_status ON end_mapped.{config.USER_ID} = prof_status.{config.USER_ID}
                 LEFT JOIN user_home ON end_mapped.{config.USER_ID} = user_home.{config.USER_ID}
                 LEFT JOIN user_work ON end_mapped.{config.USER_ID} = user_work.{config.USER_ID}
            WHERE end_mapped.total_time > 0 AND end_mapped.total_distance > 0
            """
    od_mat = bq_client.query(sql).to_dataframe()
    
    od_mat.loc[((od_mat.start_scc != od_mat.home_scc) & (od_mat.end_scc != od_mat.work_scc)) & 
       ((od_mat.start_scc != od_mat.work_scc) & (od_mat.end_scc != od_mat.home_scc)), "trip_purpose"] = "NHO"

    od_mat.loc[((od_mat.start_scc == od_mat.home_scc) & (od_mat.end_scc == od_mat.work_scc)) | 
               ((od_mat.start_scc == od_mat.work_scc) & (od_mat.end_scc == od_mat.home_scc)), "trip_purpose"] = "HBW"

    od_mat.loc[((od_mat.start_scc != od_mat.home_scc) & (od_mat.end_scc == od_mat.work_scc)) | 
           ((od_mat.start_scc == od_mat.work_scc) & (od_mat.end_scc != od_mat.home_scc)), "trip_purpose"] = "NHW"

    od_mat.loc[((od_mat.start_scc == od_mat.home_scc) & (od_mat.end_scc != od_mat.work_scc)) | 
           ((od_mat.start_scc != od_mat.work_scc) & (od_mat.end_scc == od_mat.home_scc)), "trip_purpose"] = "HBO"

    od_mat.loc[(od_mat.start_scc == od_mat.end_scc), "trip_purpose"] = "same section"
    
    od_mat_by_type = od_mat.groupby(["start_scc", "end_scc", 
                                     "professional_status",
                                     "residency_status", 
                                     "transport_mode", 
                                     "trip_purpose"]).agg(count_people = (config.USER_ID, "nunique"),
                                                          count_trips = ("trajectory_leg", "nunique"),
                                                          average_travel_time = ("total_time", "mean"),
                                                          average_travel_distance = ("total_distance", "mean")).reset_index()
    
    od_mat_by_type["datetime"] = time_start
    od_mat_by_type.rename(columns = {"start_scc": "origin_polygon_id", "end_scc": "destination_polygon_id"}, inplace=True)
    upload_table_bq(od_mat_by_type, f"{project_dataset}.{config.table_OD}", {}, bq_client)
    
    return

def BB6(time_start, time_end, project_dataset, bq_client):
    
    sql = f"""WITH consecutives AS (
              SELECT {config.USER_ID},
                  trajectory_id,
                  CONCAT(trajectory_id,'_',EXTRACT(HOUR FROM datetime_leave),'_',EXTRACT(DAY FROM datetime_leave)) AS trajectory_leg,
                  datetime_leave as start_time,
                  centroid_geography as start_point,
                  LEAD(centroid_geography) OVER (PARTITION BY {config.USER_ID}, trajectory_id ORDER BY datetime_arrive, datetime_leave ASC) AS end_point,
                  LEAD(datetime_arrive) OVER (PARTITION BY {config.USER_ID}, trajectory_id ORDER BY datetime_arrive, datetime_leave ASC) AS end_time
              FROM {project_dataset}.{config.table_stay_points}
              WHERE datetime_leave >= '{time_start}' AND datetime_leave < '{time_end}'
          ),
          s2cells AS (
              SELECT CAST(S2_CELLIDFROMPOINT(start_point, {config.s2_level}) AS STRING) as start_s2, 
                     CAST(S2_CELLIDFROMPOINT(end_point, {config.s2_level}) AS STRING) as end_s2, 
                     trajectory_id,
                     trajectory_leg,
                     {config.USER_ID},
                     ST_DISTANCE(start_point, end_point) as total_distance,
                     TIMESTAMP_DIFF(end_time, start_time, SECOND) as total_time
              FROM consecutives
            ),
            scc_mapping AS (
                SELECT DISTINCT s2code, sccode
                FROM {project_dataset}.{config.table_polygons}
                WHERE polygon_description = 'Celula'
            ),
            s2cells_mapped AS (
                SELECT s2cells.*, scc_mapping.sccode as start_scc
                FROM s2cells INNER JOIN scc_mapping
                    ON s2cells.start_s2 = scc_mapping.s2code
            ),
            users_mapped AS (
                SELECT s2cells.start_s2,
                       s2cells.end_s2,
                       s2cells.trajectory_id,
                       s2cells.trajectory_leg,
                       s2cells.{config.USER_ID},
                       s2cells.total_distance,
                       s2cells.total_time,
                       users.residential_status as residency_status
                FROM {project_dataset}.{config.table_users} AS users INNER JOIN s2cells_mapped as s2cells
                    ON users.{config.USER_ID} = s2cells.{config.USER_ID} AND users.polygon_id = s2cells.start_scc
            ),
            prof_status AS (
                SELECT {config.USER_ID}, professional_status
                FROM {project_dataset}.{config.table_user_professional}
            ),
            mode_of_transport AS (
                SELECT * FROM {project_dataset}.{config.table_mode_of_transport}
            ),
            user_home AS (
                SELECT DISTINCT {config.USER_ID}, CAST(s2code AS STRING) as s2code
                FROM {project_dataset}.{config.table_user_night_location}
                WHERE night_stay_place = 'yes'
            ),
            user_work AS (
                SELECT DISTINCT {config.USER_ID}, CAST(s2code AS STRING) as s2code
                FROM {project_dataset}.{config.table_user_work_location}
                WHERE work_place = 'yes'
            )
            SELECT users_mapped.start_s2 as start_s2, 
                   users_mapped.end_s2 as end_s2, 
                   users_mapped.{config.USER_ID}, 
                   users_mapped.trajectory_id,
                   users_mapped.trajectory_leg,
                   users_mapped.total_distance,
                   users_mapped.total_time,
                   users_mapped.residency_status,
                   prof_status.professional_status,
                   mode_of_transport.mode_of_transport as transport_mode,
                   user_home.s2code as home_s2,
                   user_work.s2code as work_s2
            FROM users_mapped
                 INNER JOIN mode_of_transport ON users_mapped.trajectory_id = mode_of_transport.trajectory_id
                 LEFT JOIN prof_status ON users_mapped.{config.USER_ID} = prof_status.{config.USER_ID}
                 LEFT JOIN user_home ON users_mapped.{config.USER_ID} = user_home.{config.USER_ID}
                 LEFT JOIN user_work ON users_mapped.{config.USER_ID} = user_work.{config.USER_ID}
            WHERE users_mapped.total_time > 0 AND users_mapped.total_distance > 0
            """
    od_mat = bq_client.query(sql).to_dataframe()
    od_mat.loc[((od_mat.start_s2 != od_mat.home_s2) & (od_mat.end_s2 != od_mat.work_s2)) & 
       ((od_mat.start_s2 != od_mat.work_s2) & (od_mat.end_s2 != od_mat.home_s2)), "trip_purpose"] = "NHO"

    od_mat.loc[((od_mat.start_s2 == od_mat.home_s2) & (od_mat.end_s2 == od_mat.work_s2)) | 
               ((od_mat.start_s2 == od_mat.work_s2) & (od_mat.end_s2 == od_mat.home_s2)), "trip_purpose"] = "HBW"

    od_mat.loc[((od_mat.start_s2 != od_mat.home_s2) & (od_mat.end_s2 == od_mat.work_s2)) | 
           ((od_mat.start_s2 == od_mat.work_s2) & (od_mat.end_s2 != od_mat.home_s2)), "trip_purpose"] = "NHW"

    od_mat.loc[((od_mat.start_s2 == od_mat.home_s2) & (od_mat.end_s2 != od_mat.work_s2)) | 
           ((od_mat.start_s2 != od_mat.work_s2) & (od_mat.end_s2 == od_mat.home_s2)), "trip_purpose"] = "HBO"

    od_mat.loc[(od_mat.start_s2 == od_mat.end_s2), "trip_purpose"] = "same section"
    
    od_mat_by_type = od_mat.groupby(["start_s2", "end_s2", 
                                     "professional_status",
                                     "residency_status", 
                                     "transport_mode", 
                                     "trip_purpose"]).agg(count_people = (config.USER_ID, "nunique"),
                                                          count_trips = ("trajectory_leg", "nunique"),
                                                          average_travel_time = ("total_time", "mean"),
                                                          average_travel_distance = ("total_distance", "mean")).reset_index()
    
    od_mat_by_type["datetime"] = time_start
    od_mat_by_type.rename(columns = {"start_s2": "origin_polygon_id", "end_s2": "destination_polygon_id"}, inplace=True)
    upload_table_bq(od_mat_by_type, f"{project_dataset}.{config.table_OD}", {}, bq_client)
    
    od_mat_total = od_mat.groupby(["start_s2", "end_s2"]).agg(count_people = (config.USER_ID, "nunique"),
                                                              count_trips = ("trajectory_id", "nunique"),
                                                              average_travel_time = ("total_time", "mean"),
                                                              average_travel_distance = ("total_distance", "mean")).reset_index()
    
    od_mat_total["datetime"] = time_start
    upload_table_bq(od_mat_total, f"{project_dataset}.{config.table_OD_totals}", {}, bq_client)
    
    return


def get_OD_mat(bq_client, project_dataset, days = None, days_of_week = None, hours = None, user_requirements = {}):
    
    where_clause = []
    if days is not None:
        where_clause.append(f"DATE(datetime) IN UNNEST({days})")
    
    if days_of_week is not None:
        days_of_week_ind = [config.friendly_weekday_reverse[day_of_week] for day_of_week in days_of_week]
        where_clause.append(f"EXTRACT(DAYOFWEEK FROM datetime) IN UNNEST({days_of_week_ind})")
        
    if hours is not None:
        where_clause.append(f"EXTRACT(HOUR FROM datetime) IN UNNEST({hours})")
        
    for user_requirement in user_requirements:
        val = user_requirements[user_requirement]
        where_clause.append(f"{user_requirement} IN UNNEST({val})")
    
    where_string = ""
    if len(where_clause) > 0:
        where_string = f"WHERE {' AND '.join(where_clause)}"
    
    sql = f"""SELECT * FROM {project_dataset}.{config.table_OD} {where_string}"""
    
    od_mat = bq_client.query(sql).to_dataframe()
    return od_mat


def get_dayofweek(day_of_week, bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, days_of_week = [day_of_week])


def get_hour(hour, bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, hours = [hour])


def get_whole_weekend(bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, days_of_week = ["Saturday", "Sunday"])


def get_morning(bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, hours = range(7,12))


def get_afternoon(bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, hours = range(12,19))


def get_night(bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, hours = list(range(20,24))+list(range(0,7)))


def get_weekdays(s2_level, bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])


def get_business_hours(s2_level, bq_client, project_dataset):
    return get_OD_mat(bq_client, project_dataset, days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"], hours = range(9,19))

