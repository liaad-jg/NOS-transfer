import utils.config as config

def finished_trajectories_query(day, time_start, time_end, raw_data_table, project_dataset):
    query = f"""WITH raw_data_table AS (
                    SELECT {', '.join([f"raw_data_table.{col}" for col in config.data_columns])}
                    FROM {raw_data_table} AS raw_data_table LEFT JOIN {project_dataset}.{config.unfinished_trajectories_table} AS unfinished 
                        ON raw_data_table.imsi = unfinished.imsi
                    WHERE 
                        day_part = DATE('{day}') AND 
                        location_data_quality_score_name IN UNNEST(['medium','high']) AND
                        rec_timestamp >= COALESCE(unfinished.first_timestamp, '{time_start}') AND rec_timestamp < '{time_end}'
                ),
                logs_before_gap AS (
                    SELECT
                        imsi,
                        MAX(rec_timestamp) AS last_timestamp_before_gap
                    FROM (
                        SELECT 
                            imsi, 
                            rec_timestamp, 
                            LEAD(rec_timestamp) OVER (PARTITION BY imsi ORDER BY rec_timestamp ASC) AS next_timestamp
                        FROM 
                            raw_data_table
                        ORDER BY 
                            imsi, 
                            rec_timestamp ASC
                    ) 
                    WHERE 
                        TIMESTAMP_DIFF(DATETIME(next_timestamp), DATETIME(rec_timestamp),  SECOND) > {config.NEW_TRAJECTORY_TIME_THRESHOLD}
                    GROUP BY
                        imsi
                ), 
                logs_before_time_end_minus_eps AS (
                    SELECT 
                        imsi
                    FROM (
                        SELECT
                            imsi,
                            MAX(rec_timestamp) AS last_timestamp
                        FROM 
                            raw_data_table
                        GROUP BY 
                            imsi
                    )
                    WHERE 
                        TIMESTAMP(last_timestamp) <= TIMESTAMP('{time_end}') - INTERVAL {config.NEW_TRAJECTORY_TIME_THRESHOLD} SECOND
                )
                SELECT DISTINCT
                    {', '.join([f"raw_data_table.{col}" for col in config.data_columns])} 
                FROM 
                    raw_data_table LEFT JOIN logs_before_gap 
                        ON raw_data_table.imsi=logs_before_gap.imsi
                WHERE 
                    raw_data_table.rec_timestamp <= logs_before_gap.last_timestamp_before_gap OR 
                    raw_data_table.imsi in (
                                        SELECT logs_before_eps.imsi 
                                        FROM logs_before_time_end_minus_eps as logs_before_eps
                                       )"""
    
    return query


def unfinished_trajectories_query(day, time_start, time_end, raw_data_table, project_dataset):
    
    unfinished_trajs_table = f"{project_dataset}.{config.unfinished_trajectories_table}"
    
    query = f"""CREATE OR REPLACE TABLE {unfinished_trajs_table} AS 
                    WITH raw_data_table AS (
                        SELECT 
                            {', '.join([f"raw_data_table.{col}" for col in config.data_columns])}
                        FROM 
                            {raw_data_table} AS raw_data_table LEFT JOIN {project_dataset}.{config.unfinished_trajectories_table} AS unfinished 
                            ON raw_data_table.imsi = unfinished.imsi
                        WHERE 
                            day_part = DATE('{day}') AND 
                            location_data_quality_score_name IN UNNEST(['medium','high']) AND
                            rec_timestamp >= COALESCE(unfinished.first_timestamp, '{time_start}') AND rec_timestamp < '{time_end}'
                    ),
                    logs_after_gap AS (
                        SELECT 
                            imsi,
                            MAX(rec_timestamp) as first_timestamp_after_gap
                        FROM (
                            SELECT 
                                imsi, 
                                rec_timestamp, 
                                LAG(rec_timestamp) OVER (PARTITION BY imsi ORDER BY rec_timestamp ASC) AS previous_timestamp
                            FROM 
                                raw_data_table
                            ORDER BY 
                                imsi, 
                                rec_timestamp ASC
                        ) AS t
                        WHERE 
                            TIMESTAMP_DIFF(DATETIME(rec_timestamp), DATETIME(previous_timestamp), SECOND) > {config.NEW_TRAJECTORY_TIME_THRESHOLD}
                        GROUP BY
                            imsi
                    ), 
                    logs_no_gaps AS (
                        SELECT imsi
                        FROM (
                            SELECT 
                                imsi,
                                rec_timestamp,
                                TIMESTAMP_DIFF(DATETIME(next_timestamp), DATETIME(rec_timestamp), SECOND) AS time_diff
                            FROM (
                                SELECT 
                                    imsi,
                                    rec_timestamp,
                                    LEAD(rec_timestamp) OVER (PARTITION BY imsi ORDER BY rec_timestamp ASC) AS next_timestamp
                                FROM  
                                    raw_data_table
                            )
                        )
                        GROUP BY 
                            imsi
                        HAVING 
                            MAX(time_diff) < {config.NEW_TRAJECTORY_TIME_THRESHOLD} OR MAX(time_diff) IS NULL
                    )
                    SELECT DISTINCT
                        raw_data_table.imsi, MIN(raw_data_table.rec_timestamp) as first_timestamp
                    FROM 
                        raw_data_table as raw_data_table LEFT JOIN logs_after_gap ON raw_data_table.imsi=logs_after_gap.imsi
                    WHERE 
                        raw_data_table.imsi NOT IN (
                            SELECT 
                                imsi
                            FROM (
                                SELECT
                                    imsi,
                                    MAX(rec_timestamp) AS last_timestamp
                                FROM 
                                    raw_data_table
                                GROUP BY 
                                    imsi
                            )
                            WHERE 
                                TIMESTAMP(last_timestamp) <= TIMESTAMP('{time_end}') - INTERVAL {config.NEW_TRAJECTORY_TIME_THRESHOLD} SECOND
                        ) AND 
                        (raw_data_table.rec_timestamp >= logs_after_gap.first_timestamp_after_gap OR 
                        raw_data_table.imsi in (SELECT imsi from logs_no_gaps))
                    GROUP BY
                        raw_data_table.imsi"""
    
    return query
