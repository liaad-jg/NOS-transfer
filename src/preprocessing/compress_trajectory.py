import numpy as np
from utils.config import USER_ID
import pandas as pd


def getDistanceByEuclidean(coords1, coords2):
    "Euclidean distance formula - give coordinates as arrays of (lat_decimal,lon_decimal) tuples"
    
    # convert the arrays to NumPy arrays and convert latitudes to radians
    coords1 = np.radians(np.asarray(coords1))
    coords2 = np.radians(np.asarray(coords2))
    
    # calculate the distance using the Euclidean formula
    dx = (coords2[:, 1] - coords1[:, 1]) * np.cos(0.5 * (coords1[:, 0] + coords2[:, 0]))
    dy = coords2[:, 0] - coords1[:, 0]
    distance = np.sqrt(dx ** 2 + dy ** 2) * 6371000  # 6371000 is the approximate radius of the earth in meters
    
    return distance


def getDistanceMatrix(coords):
    "Calculate distance matrix between all pairs of points in an array of coordinates"

    # convert the array to a NumPy array and convert latitudes to radians
    coords = np.radians(np.asarray(coords.astype(float)))

    # calculate the pairwise differences in latitude and longitude
    dlat = coords[:, 0, np.newaxis] - coords[:, 0]
    dlon = coords[:, 1, np.newaxis] - coords[:, 1]

    # calculate the distance using the Euclidean formula
    dx = dlon * np.cos(0.5 * (coords[:, 0, np.newaxis] + coords[:, 0]))
    dy = dlat
    distance = np.sqrt(dx ** 2 + dy ** 2) * 6371000  # 6371000 is the approximate radius of the earth in meters

    return distance


def check_velocity(distance_meters, time_before, time_now, max_velocity):
    time_diff = (time_now - time_before) / np.timedelta64(1, 's')
    current_velocity = distance_meters / time_diff
    return current_velocity >= max_velocity




def compress(tdf, spatial_radius_m=25, velocity_threshold_ms = 35):
    """Trajectory compression.
    
    Reduce the number of points in a trajectory for each individual in a TrajDataFrame. All points within a radius of `spatial_radius_km` kilometers from a given initial point are compressed into a single point that has the median coordinates of all points and the time of the initial point [Z2015]_.
    
    Parameters
    ----------
    tdf : TrajDataFrame
        the input trajectories of the individuals.

    spatial_radius_km : float, optional
        the minimum distance (in km) between consecutive points of the compressed trajectory. The default is `0.2`.
    
    Returns
    -------
    TrajDataFrame
        the compressed TrajDataFrame.
    
    References
    ----------
    .. [Z2015] Zheng, Y. (2015) Trajectory data mining: an overview. ACM Transactions on Intelligent Systems and Technology 6(3), https://dl.acm.org/citation.cfm?id=2743025
    """
    # Sort
    #tdf = tdf.sort_values(by=[USER_ID, "datetime"], ascending=[True, True])
    
    # Assume to be sorted when called
    
    ctdf = tdf.groupby([USER_ID], group_keys=False).apply(_compress_trajectory, spatial_radius=spatial_radius_m, velocity_threshold = velocity_threshold_ms)
    
    # TODO: remove the following line when issue #71 (Preserve the TrajDataFrame index during preprocessing operations) is solved.
    ctdf.reset_index(inplace=True, drop=True)
    return ctdf


def _compress_trajectory(tdf, spatial_radius, velocity_threshold):
    # From dataframe convert to numpy matrix
    
    columns = ["lat", "lon", "datetime"]
    columns = columns + list(set(tdf.columns) - set(columns))

    lat_lng_dtime_other = tdf.loc[:, columns].values
    columns_order = list(tdf.columns)

    compressed_traj = _compress_array(lat_lng_dtime_other, spatial_radius, velocity_threshold)

    compressed_traj = pd.DataFrame(compressed_traj, columns=columns)
    # Put back to the original order
    compressed_traj = compressed_traj[columns_order]

    return compressed_traj


def _compress_array(lat_lng_dtime_other, spatial_radius, velocity_threshold):
    if len(lat_lng_dtime_other) < 2:
        return lat_lng_dtime_other

    # Define the distance function to use
    #measure_distance = getDistanceByEuclidean
    distance_matrix = getDistanceMatrix(lat_lng_dtime_other[:, :2])
    compressed_traj = []
    lat_0, lon_0 = lat_lng_dtime_other[0][:2]

    sum_lat, sum_lon = [lat_0], [lon_0]
    t_0 = lat_lng_dtime_other[0][2]
    sum_t = [t_0]

    i_0 = 0
    last_i = i_0
    lendata = len(lat_lng_dtime_other)

    for i in range(1, lendata):
        
        lat,lon,t = lat_lng_dtime_other[i][:3]

        #Dr = measure_distance([lat_0,lon_0],[lat, lon])
        Dr = distance_matrix[i_0, i]
        
        if Dr > spatial_radius:
            
            if check_velocity(distance_matrix[last_i, i], sum_t[-1], t, velocity_threshold):
                continue
                
            extra_cols = list(lat_lng_dtime_other[i_0][3:])
            med_lat, med_lon = np.median(sum_lat), np.median(sum_lon)
            compressed_traj += [[med_lat, med_lon, sum_t[0]] + extra_cols]
            if len(sum_t) > 1:
                compressed_traj += [[med_lat, med_lon, sum_t[-1]] + extra_cols]

            i_0 = i
            sum_lat, sum_lon, sum_t = [], [], []

        sum_lat.append(lat)
        sum_lon.append(lon)
        sum_t.append(t)
        last_i = i
        

    extra_cols = list(lat_lng_dtime_other[i_0][3:])
    med_lat, med_lon = np.median(sum_lat), np.median(sum_lon)
    compressed_traj += [[med_lat, med_lon, sum_t[0]] + extra_cols]
    if len(sum_t) > 1:
        compressed_traj += [[med_lat, med_lon, sum_t[-1]] + extra_cols]

    return compressed_traj
