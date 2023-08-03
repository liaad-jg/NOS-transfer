import pandas as pd
import shapely
from utils.utils import zero_pad_results
from building_blocks.BB1 import BB1_by_datetime, BB1_by_day
import utils.config as config


def count_MOT(polygon, user_table, MOT_table, project_dataset, client, hourly = True):
    
    if hourly:
        data_in_polygon = BB1_by_datetime(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
        aggregation_column = ["day", "hour"]
    else:
        data_in_polygon = BB1_by_day(shapely.wkt.loads(polygon["geography"]), project_dataset, client)
        aggregation_column = ["date"]
        

    pp_locations = data_in_polygon.merge(user_table, on=config.USER_ID, how="left")
    pt_locations = pp_locations.merge(MOT_table, on="trajectory_id", how="left")
    
    pt_locations = pt_locations.groupby(aggregation_column + ["residential_status", "mode_of_transport"]).agg(count_trips=("trajectory_id", "nunique")).reset_index()
    
            
    if len(pt_locations) == 0:
        pt_locations = pd.DataFrame(columns = aggregation_column + ["residential_status", "mode_of_transport", "count_trips"])
        pt_locations["residential_status"] = pd.Categorical(["resident"], categories=["resident", "national tourist", 'international tourist', 
                                                                      "regular visitor", "casual visitor", "commuter"], ordered=True)
        pt_locations["mode_of_transport"] = pd.Categorical(["car"], categories = ["train",'car', 'walk', 'bus', 'bike', "stationary", "undefined"])
        pt_locations["count_trips"] = 0
        
    zero_padded = pt_locations.groupby(["residential_status", "mode_of_transport"]).apply(lambda x: zero_pad_results(x.drop(columns=["residential_status", "mode_of_transport"]), 
                                                                                              "count_trips", 
                                                                                              hourly)).reset_index()
    
    zero_padded_total_res = zero_padded.groupby(aggregation_column + ["mode_of_transport"]).agg(count_trips=("count_trips", "sum")).reset_index()
    zero_padded_total_res["residential_status"] = "total"
    
    zero_padded_total_mot = zero_padded.groupby(aggregation_column + ["residential_status"]).agg(count_trips=("count_trips", "sum")).reset_index()
    zero_padded_total_mot["mode_of_transport"] = "total"
    
    zero_padded_total = zero_padded.groupby(aggregation_column).agg(count_trips=("count_trips", "sum")).reset_index()
    zero_padded_total["mode_of_transport"] = "total"
    zero_padded_total["residential_status"] = "total"
    
    zero_padded = pd.concat([zero_padded, zero_padded_total, zero_padded_total_res, zero_padded_total_mot])
    zero_padded["polygon_id"] = polygon["polygon_id"]
    cols_order = ["polygon_id", "date", "residency_status", "mode_of_transport", "count_trips"]
    
    if hourly:
        zero_padded["datetime"] = zero_padded.apply(lambda x: f"{str(x['day'])} {x['hour']:02d}:00:00", axis=1)
        zero_padded.drop(columns = ["day", "hour"], inplace=True)
        cols_order = ["polygon_id", "datetime", "residency_status", "mode_of_transport", "count_trips"]
        zero_padded = zero_padded.loc[zero_padded.datetime < "2023-05-01 11:00:00", :]
    
    zero_padded.count_trips = zero_padded.count_trips.astype(int)
    return zero_padded.rename(columns={"residential_status": "residency_status"}).loc[:, cols_order]
