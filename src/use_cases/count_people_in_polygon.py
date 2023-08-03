import pandas as pd
import shapely
from utils.utils import zero_pad_results
from building_blocks.BB1 import BB1_by_datetime, BB1_by_day
import utils.config as config


#########################################
#
#
# Output:
# - Number of unique people in polygon by hour or by day
#
#
##########################################


def count_people_in_polygon(polygon, user_table, project_dataset, client, hourly = True):
    
    if hourly:
        people_in_polygon = BB1_by_datetime(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
        aggregation_column = ["day", "hour"]
    else:
        people_in_polygon = BB1_by_day(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
        aggregation_column = ["date"]
        
    pp_locations = people_in_polygon.merge(user_table, on=config.USER_ID, how="left")
    
    pp_locations = pp_locations.groupby(aggregation_column + ["residential_status"]).agg(number_users=(config.USER_ID, "nunique")).reset_index()
    
    zero_padded = pp_locations.groupby("residential_status").apply(lambda x: zero_pad_results(x.drop(columns=["residential_status"]), 
                                                                                              "number_users", 
                                                                                              hourly)).reset_index().drop(columns=["level_1"])
    zero_padded_total = zero_padded.groupby(aggregation_column).agg(number_users=("number_users", "sum")).reset_index()
    zero_padded_total["residential_status"] = "total"
    
    zero_padded = pd.concat([zero_padded, zero_padded_total])
    zero_padded["polygon_id"] = polygon["polygon_id"]
    cols_order = ["polygon_id", "date", "residency_status", "count_people"]
    
    if hourly:
        zero_padded["datetime"] = zero_padded.apply(lambda x: f"{str(x['day'])} {x['hour']:02d}:00:00", axis=1)
        zero_padded.drop(columns = ["day", "hour"], inplace=True)
        cols_order = ["polygon_id", "datetime", "residency_status", "count_people"]
    
    zero_padded.number_users = zero_padded.number_users.astype(int)
    return zero_padded.rename(columns={"residential_status": "residency_status", "number_users": "count_people"}).loc[:, cols_order]


def count_people_in_polygon_total(polygon, project_dataset, client, hourly = True):
    
    if hourly:
        people_in_polygon = BB1_by_datetime(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
        aggregation_column = ["day", "hour"]
    else:
        people_in_polygon = BB1_by_day(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
        aggregation_column = ["date"]
        
        
    pp_locations = people_in_polygon.groupby(aggregation_column).agg(number_users=(config.USER_ID, "nunique")).reset_index()
    
    zero_padded = zero_pad_results(pp_locations, "number_users", hourly)
    
    zero_padded["residential_status"] = "total"
    
    zero_padded["polygon_id"] = polygon["polygon_id"]
    
    cols_order = ["polygon_id", "date", "residency_status", "count_people"]
    if hourly:
        zero_padded["datetime"] = zero_padded.apply(lambda x: f"{str(x['day'])} {x['hour']:02d}:00:00", axis=1)
        zero_padded.drop(columns = ["day", "hour"], inplace=True)
        cols_order = ["polygon_id", "datetime", "residency_status", "count_people"]
    
    zero_padded.number_users = zero_padded.number_users.astype(int)
    return zero_padded.rename(columns={"residential_status": "residency_status", "number_users": "count_people"}).loc[:, cols_order]