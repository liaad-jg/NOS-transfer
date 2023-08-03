from building_blocks.BB1 import BB1_by_datetime, BB1_by_day
from utils.utils import align_user_table
import shapely
import pandas as pd
from utils.utils import zero_pad_results
import utils.config as config


def group_by_catchment_area(df, polygon):
    
    catchment_area_labels = ["local", "regional", "national", "international"]

    international = len(df[(df.residential_status_x == "international tourist") |
                          (df.mcc != "268")].imsi.unique())
    df_no_int_tourist = df[(df.residential_status_x != "international_tourist") & (df.mcc == "268")]
    national = len(df_no_int_tourist[(df_no_int_tourist.dicode.isna()) | (df_no_int_tourist.dicode != polygon.dicode)].imsi.unique())
    regional = len(df_no_int_tourist[(df_no_int_tourist.dicode == polygon.dicode) & (df_no_int_tourist.cocode != polygon.cocode)].imsi.unique())
    local = len(df_no_int_tourist[(df_no_int_tourist.cocode == polygon.cocode)].imsi.unique())
    
    return pd.DataFrame.from_dict(dict(catchment_area = catchment_area_labels, count_people = [local, regional, national, international]))
    

def catchment_area(polygon, user_table, project_dataset, client):
    
    people_in_polygon = BB1_by_datetime(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
    pol_relevant_user_table = align_user_table(polygon, user_table)
    
    pp_locations = people_in_polygon.merge(pol_relevant_user_table, on=config.USER_ID, how="left")
    
    pp_locations = pp_locations.merge(user_table.loc[:, [config.USER_ID, "mcc"]].drop_duplicates(), on=config.USER_ID, how="left")
    
    pp_locations_non_residents = pp_locations.loc[~pp_locations.residential_status.isin(["resident", "commuter"]), :]
    
    labels_resident_location = pp_locations_non_residents.merge(user_table[(user_table.imsi.isin(pp_locations_non_residents.imsi.unique())) & 
                                                                           (user_table.residential_status == "resident")], on="imsi", how="left")
    
    catchment_levels = labels_resident_location.groupby(["day", "hour"]).apply(lambda x: group_by_catchment_area(x, polygon)).reset_index()
    if len(catchment_levels) == 0:
        catchment_levels["catchment_area"] = pd.Categorical([],categories=["local", "regional", "national", "international"])
        catchment_levels["day"] = pd.Timestamp(config.start_datetime_demo_prev).date()
        catchment_levels["hour"] = 0
        catchment_levels["count_people"] = 0
        catchment_levels.drop(columns = "index", inplace=True)
        
    
    zero_padded = catchment_levels.groupby("catchment_area").apply(lambda x: zero_pad_results(x.drop(columns=["catchment_area"]), 
                                                                                              "count_people", 
                                                                    True)).reset_index()
    
    zero_padded.count_people = zero_padded.count_people.astype(int)
    
    zero_padded["datetime"] = zero_padded.apply(lambda x: f"{str(x['day'])} {x['hour']:02d}:00:00", axis=1)
    zero_padded["polygon_id"] = polygon["polygon_id"]
    
    return zero_padded.rename(columns = {"catchment_area": "c_area"}).loc[:, ["polygon_id", "datetime", "c_area", "count_people"]]


def catchment_area_daily(polygon, user_table, project_dataset, client):
    
    people_in_polygon = BB1_by_day(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
    pol_relevant_user_table = align_user_table(polygon, user_table)
    
    pp_locations = people_in_polygon.merge(pol_relevant_user_table, on=config.USER_ID, how="left")
    
    pp_locations = pp_locations.merge(user_table.loc[:, [config.USER_ID, "mcc"]].drop_duplicates(), on=config.USER_ID, how="left")
    
    pp_locations_non_residents = pp_locations.loc[~pp_locations.residential_status.isin(["resident", "commuter"]), :]
    
    labels_resident_location = pp_locations_non_residents.merge(user_table[(user_table.imsi.isin(pp_locations_non_residents.imsi.unique())) & 
                                                                           (user_table.residential_status == "resident")], on="imsi", how="left")
    
    catchment_levels = labels_resident_location.groupby(["date"]).apply(lambda x: group_by_catchment_area(x, polygon)).reset_index()
    if len(catchment_levels) == 0:
        catchment_levels["catchment_area"] = pd.Categorical([],categories=["local", "regional", "national", "international"])
        catchment_levels["date"] = pd.Timestamp(config.start_datetime_demo_prev).date()
        catchment_levels["count_people"] = 0
        catchment_levels.drop(columns = "index", inplace=True, errors = "ignore")
        
    
    zero_padded = catchment_levels.groupby("catchment_area").apply(lambda x: zero_pad_results(x.drop(columns=["catchment_area", "level_1"], errors="ignore"), 
                                                                                              "count_people", False)).reset_index()
    
    zero_padded.count_people = zero_padded.count_people.astype(int)
    zero_padded["polygon_id"] = polygon["polygon_id"]
    
    return zero_padded.rename(columns = {"catchment_area": "c_area"}).loc[:, ["polygon_id", "date", "c_area", "count_people"]]
