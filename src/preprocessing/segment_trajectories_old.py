import utils.config as config
import pandas as pd
import time
from compress_trajectory import compress
import numpy as np
from numpy.lib.stride_tricks import sliding_window_view


def sort_df_values(df):
    df = df.sort_values(by=[config.USER_ID, 'datetime'], ascending=[True, True])
    return df


def group_same_datetime(df):
    df[['median_lat', 'median_lon']] = df.groupby([config.USER_ID, 'datetime'])[['lat', 'lon']].transform('median')
    df = df.drop_duplicates(subset=[config.USER_ID,"datetime"]).drop(columns=["lat","lon"]).rename(columns = {"median_lat":"lat", "median_lon":"lon"})
    return df


def _calculate_dates_times(df):

    df['seconds_since_prior'] = df.datetime.diff().dt.total_seconds().fillna(0)
    df['seconds_to_next'] = np.abs(df.datetime.diff(-1).dt.total_seconds().fillna(0))

    return df


def calculate_dates_times(df, cols_to_group = [config.USER_ID]):
    # Here we calculate the dates, times
    return df.groupby(cols_to_group).apply(_calculate_dates_times).reset_index(drop=True)


def calculate_points_distance_velocity_acceleration(df):
    # Here we calculate the distances
    return df.groupby([config.USER_ID, "trajectory_id"]).apply(_calculate_points_distance_velocity_acceleration).reset_index(drop=True)


def _calculate_points_distance_velocity_acceleration(df):
    
    padded = np.pad(df[["lat","lon"]].values, ((4,0), (0,0)), mode="edge")
    slidding_padded = sliding_window_view(padded, (5, 2)).squeeze(1)[::1, :, :]
    median_lat = np.median(slidding_padded[:, :, 0], axis=1)
    median_lon = np.median(slidding_padded[:, :, 1], axis=1)

    # Combine the median latitudes and longitudes into a single array
    median_coordinates = np.column_stack((median_lat, median_lon))

    df["lat"] = median_coordinates[:, 0]
    df["lon"] = median_coordinates[:, 1]

    #Create two new columns to calculate the distance
    df['lat_next_position'] = df['lat'].shift(-1)   
    df.loc[df.lat_next_position.isna(), "lat_next_position"] = df.loc[df.lat_next_position.isna(), "lat"]
    df['lon_next_position'] = df['lon'].shift(-1)
    df.loc[df.lon_next_position.isna(), "lon_next_position"] = df.loc[df.lon_next_position.isna(), "lon"]

    #Divide the result by 1000 to obtain the distance in KM
    #df['distance'] = [gpDist([lat, lon], [lat_n, lon_n]).meters for lat, lon, lat_n, lon_n in zip(df.lat.values, df.lon.values,
    #                                                                                                 df.lat_next_position.values, df.lon_next_position.values)]
    
    df['distance'] = df.apply(lambda x: config.distance_function((x['lat'], x['lon']), 
                                                                 (x['lat_next_position'], x['lon_next_position'])).meters, axis=1)
    
    df['velocity'] = df['distance'] / df.seconds_to_next

    df['velocity_next_position'] = df['velocity'].shift(-1)
    df['acceleration'] = (df['velocity_next_position'] - df['velocity']) / df.seconds_to_next
    
    
    df['next_geography'] = [f"POINT({lon} {lat})" for lat, lon in zip(df.lat_next_position.values, df.lon_next_position.values)]
    df.drop(['lat_next_position','lon_next_position', "velocity_next_position"], axis=1, inplace=True)
    
    return df


def _trajectories_temporal_segmentation(df):
    
    df.reset_index(drop=True, inplace=True)

    threshold_filter_indexes = df[df.seconds_since_prior >= config.NEW_TRAJECTORY_TIME_THRESHOLD].index
        
    traj_id = lambda time_part: f"{df[config.USER_ID].iloc[0]}_{time_part.strftime(config.TRAJECTORY_CONVERT_TIME_FORMAT)}"
        
    start = 0
    
    for index in threshold_filter_indexes:
        df.loc[(df.index >= start) & (df.index < index), "trajectory_id"] = traj_id(df.datetime.iloc[start])
        start = index
        
    df.loc[df.index >= start, "trajectory_id"] = traj_id(df.datetime.iloc[start])
            
    return df


def trajectories_temporal_segmentation(df):
    """Cluster the points of a TrajDataFrame into trajectories by using time threshold.

    Parameters
    ----------
    tdf : TrajDataFrame
        original trajectories

    Returns
    -------
    TrajDataFrame
        the TrajDataFrame with a new column 'trajectory_id' collecting the unique identifier of the trajectory to which the point
        belongs.
    """
    # if we pass multiple users to this function, uncomment the following line
    
    return df.groupby(config.USER_ID).apply(_trajectories_temporal_segmentation).reset_index(drop=True)


def assign_trajectory_id(df):
    
    traj_id = f"{df[config.USER_ID].iloc[0]}_{df.datetime.iloc[0].strftime(config.TRAJECTORY_CONVERT_TIME_FORMAT)}"
    df["trajectory_id"] = traj_id
    return df


def _get_trajectory_statistics(df):
    total_distance = df.distance.sum()
    total_time = (df.datetime.iloc[-1] - df.datetime.iloc[0]).seconds
    average_speed = total_distance/total_time if total_time > 0 else 0
    average_acceleration = average_speed/total_time if total_time > 0 else 0
    stats_dict = dict(total_time = [total_time], 
                      distance_difference = [config.distance_function([df.lat.iloc[0], df.lon.iloc[0]], [df.lat.iloc[-1], df.lon.iloc[-1]]).meters],
                      total_distance = [total_distance],
                      average_speed = [average_speed],
                      average_acceleration = [average_acceleration],
                      n_points = len(df),
                      start_point = [df.geography.iloc[0]],
                      end_point = [df.geography.iloc[-1]], 
                      start_time = [df.datetime.iloc[0]], 
                      end_time = [df.datetime.iloc[-1]],
                      trajectory_id = [df.trajectory_id.iloc[0]])
    stats_dict[config.USER_ID] = df[config.USER_ID].iloc[0] 
    
    return pd.DataFrame.from_dict(stats_dict)


def get_trajectory_statistics(df):
    return df.groupby([config.USER_ID, "trajectory_id"]).apply(_get_trajectory_statistics).reset_index(drop=True)


def process_finished_trajectories(df):
    
    df = df.rename(columns={'rec_timestamp': 'datetime'})
    df['datetime'] = pd.to_datetime(df["datetime"])
    df["lat"] = df.lat.astype(float)
    df["lon"] = df.lon.astype(float)
    
    start = time.time()
    df = sort_df_values(df)
    df = group_same_datetime(df)
    df = calculate_dates_times(df)
    print(f"Finished assigning times: {time.time()-start:.2f}s")
    
    users_with_multiple_trajectories = df.loc[df["seconds_to_next"] > config.NEW_TRAJECTORY_TIME_THRESHOLD, config.USER_ID].values
    trajectories_to_cut = df.loc[df[config.USER_ID].isin(users_with_multiple_trajectories), :]
    whole_trajectories = df.loc[~df[config.USER_ID].isin(users_with_multiple_trajectories), :]
    
    start = time.time()
    
    whole_trajectories = whole_trajectories.groupby(config.USER_ID).apply(assign_trajectory_id).reset_index(drop=True)
    
    print(f"Finished assigning trajectory IDs: {time.time()-start:.2f}s")
    
    start = time.time()
    
    trajectories_to_cut = trajectories_temporal_segmentation(trajectories_to_cut)
    
    print(f"Finished segmenting trajectories: {time.time()-start:.2f}s")
    
    df = pd.concat([whole_trajectories, trajectories_to_cut])
    
    start = time.time()
    
    df = compress(df, config.COMPRESSION_THRESHOLD, config.VELOCITY_THRESHOLD) # distances are in meters 
    
    print(f"Finished compressing: {time.time()-start:.2f}s")
    
    start = time.time()
    
    df['geography'] = [f"POINT({lon} {lat})" for lat, lon in zip(df.lat.values, df.lon.values)]
    df = calculate_dates_times(df, cols_to_group = [config.USER_ID, "trajectory_id"])
    print(f"Finished assigning times: {time.time()-start:.2f}s")
    
    start = time.time()  
    df = calculate_points_distance_velocity_acceleration(df)   
    print(f"Finished distances: {time.time()-start:.2f}s")
    
    start = time.time()
    finished_trajectories_summary = get_trajectory_statistics(df)
    print(f"Finished stats: {time.time()-start:.2f}s")
    
    return df, finished_trajectories_summary
