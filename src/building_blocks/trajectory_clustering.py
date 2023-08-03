import numpy as np
import pandas as pd
import similaritymeasures
import utm_conversion as utm
import utils.config as config
from rdp import rdp
from dbscan_kmeans import DBScanKmeans
from sklearn.cluster import DBSCAN
from h3 import h3
import utils.utils as utils
import math
from shapely.ops import transform
from shapely.geometry import  LineString
from datetime import datetime

class TrajectoryClustering:

    df_move = None
    df_cluster = None
    df_logs = None

    @classmethod
    def get_trajectories(cls, cluster_ini, cluster_end, df_move, df_cluster, df_logs):
           
        cls.df_move = df_move
        cls.df_cluster = df_cluster       
        cls.df_logs = df_logs

        _df_m = cls.df_move
        _df_l = cls.df_logs

        _f1 = _df_m["cluster_ini"] == cluster_ini
        _f2 = _df_m["cluster_end"] == cluster_end

        _moves = cls.df_move.loc[_f1 & _f2]

        _trajectories = []
        for idx, _m in _moves.iterrows():
            _user_id = _m["user_id"]

            try:
                _trajectory_id = _m["trajectory_id"]
                _c1 = _df_l["trajectory_id"] == _trajectory_id
            except:
                _datetime = _m["datetime_ini"]
                _c1 = _df_l["datetime"] == _datetime

            _c2 = _df_l["user_id"] == _user_id
            _c4 = _df_l["lat"] != _df_l["lat"].shift(1)
            _c5 = _df_l["lon"] != _df_l["lon"].shift(1)

            _cols = ["user_id", "trajectory_id", "datetime", "lat", "lon"]

            _df_v_l = _df_l.loc[_c1 & _c2 & _c4 & _c5, _cols]

            _cols = ["lat", "lon"]
            if not _df_v_l.empty:
                _trajectories.append(_df_v_l[_cols].values)
        return _trajectories

    @classmethod
    def compute_distance_matrix(cls, trajectories, method="Frechet"):
        """
        :param method: "Frechet" or "Area"
        """
        n = len(trajectories)
        dist_m = np.zeros((n, n))
        for i in range(n - 1):
            p = trajectories[i]
            for j in range(i + 1, n):
                q = trajectories[j]
                if method == "Frechet":
                    dist_m[i, j] = similaritymeasures.frechet_dist(p, q)

                else:
                    dist_m[i, j] = similaritymeasures.area_between_two_curves(p, q)
                dist_m[j, i] = dist_m[i, j]
        return dist_m

    @classmethod
    def reduce_polyline_points_by_rdp(cls, polyline, epsilon=10, return_indices=False):
        """
        :param polyline:
        :param epsilon: unit m for Frechet distance, m^2 for Area
        :param return_indices: boolean
        """
        point_list = polyline.tolist()
        points = rdp(point_list, epsilon=epsilon)
        return np.array(points)

    @classmethod
    def clustering_by_dbscan(cls, distance_matrix, min_samples=5, eps=1000):
        """
        :param eps: unit m for Frechet distance, m^2 for Area
        """
        cl = DBSCAN(eps=eps, min_samples=min_samples, metric='precomputed')
        cl.fit(distance_matrix)
        return cl.labels_

    @staticmethod
    def geo_to_h3(row, lat_column, lon_column):
        return h3.geo_to_h3(lat=row[lat_column], lng=row[lon_column], resolution=config.H3_res)

    @classmethod
    def get_gps_logs_moves(cls, df_gps_logs):

        grouped_trajs = df_gps_logs.groupby('trajectory_id')
        move_days_columns = ['user_id', 'trajectory_id', 'lat_ini', 'lon_ini', 'lat_end', 'lon_end', 'datetime_ini',
                             'datetime_end']

        df_moves = pd.DataFrame(columns=move_days_columns)

        trajs = []
        for group in grouped_trajs.groups:
            traj = grouped_trajs.get_group(group)
            trajs.append([traj.user_id.iloc[0], traj.trajectory_id.iloc[0],
                          traj.lat.iloc[0], traj.lon.iloc[0], traj.lat.iloc[-1], traj.lon.iloc[-1],
                          traj.datetime.iloc[0], traj.datetime.iloc[-1]])

        df_moves = pd.concat([df_moves, pd.DataFrame(trajs, columns=move_days_columns)], ignore_index=True)
        #df_moves['trajectory_id'] = df_moves['trajectory_id'].astype('int64')

        df_moves.loc[:, 'h3_ini'] = df_moves.apply(lambda row: cls.geo_to_h3(row, 'lat_ini', 'lon_ini'), axis=1)
        df_moves.loc[:, 'h3_end'] = df_moves.apply(lambda row: cls.geo_to_h3(row, 'lat_end', 'lon_end'), axis=1)

        return df_moves

    @classmethod
    def get_moves_start_end_locations(cls, df_moves):
        loc_ini = df_moves[['lat_ini', 'lon_ini']].to_numpy()
        loc_end = df_moves[['lat_end', 'lon_end']].to_numpy()
        locations = np.vstack((loc_ini, loc_end))

        return locations

    @classmethod
    def get_moves_start_end_locations_clusters_db_means(cls, start_end_locations_lat_lon,
                             dbscan_kmeans_eps_in_degrees=config.dbscan_kmeans_eps_in_degrees,
                             dbscan_kmeans_min_samples=config.dbscan_kmeans_min_samples):
        
        
        eps = dbscan_kmeans_eps_in_degrees

        dbscan_kmeans_ = DBScanKmeans(eps=eps,
                                      MinPts=dbscan_kmeans_min_samples)

        _ = dbscan_kmeans_.fit_and_predict(start_end_locations_lat_lon)
        
        return dbscan_kmeans_

    @classmethod
    def get_moves_start_end_locations_clusters_db_scan(cls, locations, eps_in_meters=50.0, num_samples=10):
        pts = np.radians(locations)

        # Cluster the data
        earth_perimeter = 40070000.0  # In meters
        eps_in_radians = eps_in_meters / earth_perimeter * (2 * math.pi)

        clusters = DBSCAN(eps=eps_in_radians,
                          min_samples=num_samples,
                          metric='haversine',
                          algorithm='ball_tree').fit_predict(pts)

        return clusters

    @classmethod
    def get_df_clusters(cls, moves_start_end_locations, clusters_db_means):        
        df_clusters = pd.DataFrame(moves_start_end_locations, columns=['lat', 'lon'])
        df_clusters.loc[:, 'cluster_id'] = clusters_db_means.clusters_labels

        for cluster in df_clusters.cluster_id.unique():
            # print(f'The cluster is: {clusters.clusters[cluster]}')
            df_clusters.loc[df_clusters['cluster_id'] == cluster, 'cluster_lat'] = clusters_db_means.clusters[cluster][0]
            df_clusters.loc[df_clusters['cluster_id'] == cluster, 'cluster_lon'] = clusters_db_means.clusters[cluster][1]

        return df_clusters

    @classmethod
    def set_clusters_ids_to_moves(cls, df_clusters, df_moves):


        n = df_clusters.shape[0] // 2 

        df_moves.loc[:, 'cluster_ini'] = df_clusters['cluster_id'][:n].values
        df_moves.loc[:, 'cluster_end'] = df_clusters.loc[:, ['cluster_id']][n:].values

        # return only the moves between clusters (-1 stands for noise)
        df_moves = df_moves.loc[(df_moves['cluster_ini'] != -1) & (df_moves['cluster_end'] != -1), :]

        # Here we select only trajectories connecting different origins and destinations
        df_moves = df_moves[df_moves.cluster_ini != df_moves.cluster_end]

        return df_moves

    @classmethod
    def filter_clusters(cls, df_clusters):
        df_clusters = df_clusters.loc[(df_clusters['cluster_id'] != -1), :]        
        df_clusters.loc[:, 'h3'] = df_clusters.apply(lambda row: utils.geo_to_h3(row, 'cluster_lat', 'cluster_lon'), axis=1)        
        df_clusters = df_clusters[['cluster_id', 'h3', 'cluster_lat', 'cluster_lon']]

        df_clusters = df_clusters.rename(columns={"cluster_lat": "lat", "cluster_lon": "lon"})

        return df_clusters

    @classmethod
    def get_max_moves(cls, df_moves):
        df_max_moves = df_moves[['cluster_ini', 'cluster_end']]\
            .groupby(['cluster_ini', 'cluster_end']).size()\
            .rename('count_moves').reset_index()\
            .sort_values(by=['count_moves'], ascending=[False])

        return df_max_moves

    @classmethod
    def get_top_n_clusters_pairs(cls, df_max_moves, num_clusters_to_analyze=config.freq_traj_num_clusters_to_analyze):

        top_n_clusters_pairs = df_max_moves[['cluster_ini', 'cluster_end']].head(num_clusters_to_analyze).values.tolist()

        return top_n_clusters_pairs
    
    
    @classmethod
    def get_representative_cluster_trajectory(cls, day, hour, origin, destination, all_trips_logs, labels):
        
        def flip(x, y):
            """Flips the x and y coordinate values"""
            return y, x

        #print(f'The frequent trajectories are :{all_trips_logs}')    
        
        frequent_trajectories_columns = ['polyline', 'datetime', 'origin_polygon_id', 'destination_polygon_id', 'count_trips']
        df_frequent_trajectories = pd.DataFrame(columns=frequent_trajectories_columns)
        
        # Get the most representative cluster of od_trajectories
        top_frequent_cluster = np.argmax(np.bincount(labels))

        # Get the most representative cluster of od_trajectories
        count_frequent_cluster = np.bincount(labels)[top_frequent_cluster]

        frequent_trajectories = [all_trips_logs[i] for i in labels if i == top_frequent_cluster]
        
        longest_frequent_trajectory = max(frequent_trajectories, key=len)
        
        if len(frequent_trajectories) > 0 and len(longest_frequent_trajectory) > 1:
            #linestring_wkt = LineString(longest_frequent_trajectory).wkt
            linestring_wkt = transform(flip, LineString(longest_frequent_trajectory)).wkt
        # For the cases where the trajecrory is just one point (????) we have to duplicate it
        elif len(longest_frequent_trajectory) == 1:
            print(f'The trajectory has only one point ???? - returing empty dataset')
            return df_frequent_trajectories
            #linestring_wkt = LineString([longest_frequent_trajectory, longest_frequent_trajectory]).wkt
        else:
            print(f'Using the all_trips_logs first trajectory - len {len(all_trips_logs)}')                    
            #linestring_wkt = LineString(max(frequent_trajectories, key=len)).wkt
            linestring_wkt = transform(flip, LineString(max(frequent_trajectories, key=len))).wkt

        frequent_trajectories_dict = {'polyline': [linestring_wkt],
                                      'datetime': [datetime.strptime(f'{day} {hour}:00:00', '%Y-%m-%d %H:%M:%S')],
                                      'origin_polygon_id': [origin],
                                      'destination_polygon_id': [destination],
                                      'count_trips': [count_frequent_cluster]}
        df_frequent_trajectories = pd.DataFrame(data=frequent_trajectories_dict)

        #return top_frequent_cluster, count_frequent_cluster, frequent_trajectories[0], linestring_wkt
        return df_frequent_trajectories
  