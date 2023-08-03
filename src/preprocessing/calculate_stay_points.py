import pandas as pd
from skmob.preprocessing import detection
import skmob
from utils.config import USER_ID


def _get_first_last_sp(traj_df):
    
    traj_id = [traj_df.trajectory_id.iloc[0], traj_df.trajectory_id.iloc[0]]
    lats = [traj_df.lat.iloc[0], traj_df.lat.iloc[-1]]
    lons = [traj_df.lon.iloc[0],traj_df.lon.iloc[-1]]
    geographies = [traj_df.geography.iloc[0], traj_df.geography.iloc[-1]]
    start_times = [traj_df.datetime.iloc[0], traj_df.datetime.iloc[-1]]
    end_times = [traj_df.datetime.iloc[0], traj_df.datetime.iloc[-1]]
    stay_time = [0, 0]
    
    dictionary = dict(trajectory_id = traj_id, 
                      lat = lats, 
                      lon = lons, 
                      datetime_arrive = start_times, 
                      datetime_leave = end_times, 
                      stay_time = stay_time, 
                      centroid_geography = geographies)
    
    dictionary[USER_ID] = [traj_df[USER_ID].iloc[0], traj_df[USER_ID].iloc[0]]
    return pd.DataFrame.from_dict(dictionary)


def get_first_last_sp(df):
    return df.groupby(["trajectory_id"]).apply(_get_first_last_sp).reset_index(drop=True)



def calculate_stay_points(df):
    '''
    Find stay points in trajectories.
    
    '''
    
    traj_df = skmob.TrajDataFrame(df, datetime="datetime", user_id = USER_ID, longitude="lon", trajectory_id = "trajectory_id")
    sps = detection.stay_locations(traj_df)
    sps["centroid_geography"] = [f"POINT({lon} {lat})" for lat, lon in zip(sps.lat.values, sps.lng.values)]
    sps = sps.loc[:,['datetime', 'lat', 'lng', 'uid','tid','leaving_datetime', 'centroid_geography']]
    sps.rename({"datetime": 'datetime_arrive', 'leaving_datetime': 'datetime_leave', "lng": 'lon',
                'uid': USER_ID, "tid": 'trajectory_id'}, inplace=True, axis=1)
    sps["stay_time"] = (sps["datetime_leave"] - sps["datetime_arrive"]).dt.seconds
    final_sps = pd.concat([sps, get_first_last_sp(df)], ignore_index = True)
    
    return final_sps
