import pandas as pd
import utils.config as config
from utils.utils import zero_pad_results_single_day

def share_of_time(supermarkets, polygon, user_table, day):
    
    if len(supermarkets) == 0:
        
        return None
        
    
    time_with_labels = supermarkets.merge(user_table, on=config.USER_ID, how="left")
    time_with_labels.residential_status.fillna("commuter", inplace=True)
    
    share_of_time = time_with_labels.groupby(["datetime", "grupo", "residential_status"]).agg(total_time = ("stay_time", "sum")).reset_index()
    share_of_time_total = time_with_labels.groupby(["datetime", "grupo"]).agg(total_time = ("stay_time", "sum")).reset_index()
    share_of_time_total["residential_status"] = "total"
    share_of_time = pd.concat([share_of_time, share_of_time_total])
    zero_padded = share_of_time.groupby(["grupo", 
                                         "residential_status"]).apply(lambda x: zero_pad_results_single_day(x.drop(columns=["grupo", 
                                                                                                                            "residential_status"]), 
                                                                                                            "total_time")).reset_index().drop(columns=["level_2"])
    
    zero_padded['share_of_time_percentage'] = zero_padded.groupby(['datetime', 'residential_status'])['total_time'].transform(lambda x: x / x.sum())
    zero_padded["polygon_id"] = polygon["polygon_id"]
    
    zero_padded.rename(columns={"residential_status": "residency_status", "total_time": "seconds_in_store"}, inplace = True)
    
    return zero_padded.loc[:, ["polygon_id", "datetime", "residency_status", "share_of_time_percentage", "grupo", "seconds_in_store"]]