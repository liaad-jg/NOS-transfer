import utils.config as config
from utils.utils import check_coordinates_order, get_reverse_polygon
import shapely
import pandas as pd


def place_of_origin_query_by_datetime(polygon, project_dataset):
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
        
    sql = f"""WITH frc_mapping AS (
                SELECT DISTINCT s2code, frnome
                FROM {project_dataset}.{config.table_polygons}
            ),
            user_home AS (
                SELECT DISTINCT night_loc.{config.USER_ID}, frc_mapping.frnome as home_frc
                FROM {project_dataset}.{config.table_user_night_location} as night_loc INNER JOIN frc_mapping
                     ON CAST(night_loc.s2code AS STRING) = frc_mapping.s2code
                WHERE night_loc.night_stay_place = 'yes'
            ),
            user_mcc AS (
                SELECT DISTINCT users.{config.USER_ID}, table_mcc.country as country
                FROM {project_dataset}.{config.table_users} as users INNER JOIN {project_dataset}.{config.table_mcc} as table_mcc
                        ON CAST(users.mcc AS STRING) = CAST(table_mcc.mcc AS STRING)
            )
            SELECT DISTINCT DATE(datetime) as day, 
                            EXTRACT(HOUR FROM datetime) as hour, 
                            traj.{config.USER_ID},
                            user_mcc.country,
                            user_home.home_frc
            FROM {project_dataset}.{config.table_trajectory_details} as traj
                 LEFT JOIN user_home ON traj.{config.USER_ID} = user_home.{config.USER_ID}
                 LEFT JOIN user_mcc ON traj.{config.USER_ID} = user_mcc.{config.USER_ID}
            WHERE ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), traj.geography) AND 
                  datetime <= '{config.end_datetime_demo_prev}'
          """
    
    return sql


def place_of_origin_query_by_date(polygon, project_dataset):
    
    if check_coordinates_order(polygon.__geo_interface__["coordinates"][0]):
        polygon = get_reverse_polygon(polygon)
        
    sql = f"""WITH frc_mapping AS (
                SELECT DISTINCT s2code, frnome
                FROM {project_dataset}.{config.table_polygons}
            ),
            user_home AS (
                SELECT DISTINCT night_loc.{config.USER_ID}, frc_mapping.frnome as home_frc
                FROM {project_dataset}.{config.table_user_night_location} as night_loc INNER JOIN frc_mapping
                     ON CAST(night_loc.s2code AS STRING) = frc_mapping.s2code
                WHERE night_loc.night_stay_place = 'yes'
            ),
            user_mcc AS (
                SELECT DISTINCT users.{config.USER_ID}, table_mcc.country as country
                FROM {project_dataset}.{config.table_users} as users INNER JOIN {project_dataset}.{config.table_mcc} as table_mcc
                        ON CAST(users.mcc AS STRING) = CAST(table_mcc.mcc AS STRING)
            )
            SELECT DISTINCT DATE(datetime) as date, 
                            traj.{config.USER_ID},
                            user_mcc.country,
                            user_home.home_frc
            FROM {project_dataset}.{config.table_trajectory_details} as traj
                 LEFT JOIN user_home ON traj.{config.USER_ID} = user_home.{config.USER_ID}
                 LEFT JOIN user_mcc ON traj.{config.USER_ID} = user_mcc.{config.USER_ID}
            WHERE ST_INTERSECTS(ST_GEOGFROMTEXT('{polygon}'), traj.geography) AND 
                  datetime <= '{config.end_datetime_demo_prev}' 
          """
    
    return sql


def get_place_of_origin(polygon, user_table, project_dataset, client, hourly = True):
    
    if hourly:
        people_in_polygon = client.query(place_of_origin_query_by_datetime(shapely.wkt.loads(polygon["geography"]), project_dataset)).to_dataframe()
        aggregation_column = ["day", "hour"]
    else:
        people_in_polygon = client.query(place_of_origin_query_by_date(shapely.wkt.loads(polygon["geography"]), project_dataset)).to_dataframe()
        aggregation_column = ["date"]
        
        
    if len(people_in_polygon) == 0:
        return None
    
    
    pp_locations = people_in_polygon.merge(user_table, on=config.USER_ID, how="left")
    pp_locations.residential_status.fillna("commuter", inplace=True)
    
    pp_locations_foreigners = pp_locations.loc[(pp_locations.residential_status == "international tourist") | (pp_locations.home_frc.isna()), :]
    
    pp_locations_foreigners = pp_locations_foreigners.groupby(aggregation_column + ["residential_status", "country"]).agg(number_users=(config.USER_ID, "nunique")).reset_index()
    
    pp_locations_foreigners_total = pp_locations_foreigners.groupby(aggregation_column + ["country"]).agg(number_users = ("number_users", "sum")).reset_index()
    pp_locations_foreigners_total["residential_status"] = "total"
    
    pp_locations_foreigners = pd.concat([pp_locations_foreigners, pp_locations_foreigners_total])
    
    pp_locations_nationals = pp_locations.loc[(pp_locations.residential_status != "international tourist") & (~pp_locations.home_frc.isna()), :]
    
    pp_locations_nationals = pp_locations_nationals.groupby(aggregation_column + ["residential_status", "home_frc"]).agg(number_users=(config.USER_ID, "nunique")).reset_index()
    
    pp_locations_nationals_total = pp_locations_nationals.groupby(aggregation_column + ["home_frc"]).agg(number_users = ("number_users", "sum")).reset_index()
    pp_locations_nationals_total["residential_status"] = "total"
    
    pp_locations_nationals = pd.concat([pp_locations_nationals, pp_locations_nationals_total])
    
    
    place_of_origin = pd.concat([pp_locations_foreigners.rename(columns = {"country": "place_of_origin"}), 
                                 pp_locations_nationals.rename(columns = {"home_frc": "place_of_origin"})])
    
    place_of_origin["polygon_id"] = polygon["polygon_id"]
    cols_order = ["polygon_id", "date", "residency_status", "place_of_origin", "count_people"]
    
    if hourly:
        place_of_origin["datetime"] = place_of_origin.apply(lambda x: f"{str(x['day'])} {x['hour']:02d}:00:00", axis=1)
        place_of_origin.drop(columns = ["day", "hour"], inplace=True)
        cols_order = ["polygon_id", "datetime", "residency_status", "place_of_origin", "count_people"]
    
    place_of_origin.number_users = place_of_origin.number_users.astype(int)
    return place_of_origin.rename(columns={"residential_status": "residency_status", "number_users": "count_people"}).loc[:, cols_order]
