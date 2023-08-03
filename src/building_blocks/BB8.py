import pandas as pd
import utils.config as config
import s2cell
from trajectory_clustering import TrajectoryClustering

def process_frequent_trajectories(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client):
    
    df_gps_logs = get_gps_logs(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client)
    
    frequent_trajectories_columns = ['polyline', 'datetime', 'origin_polygon_id', 'destination_polygon_id', 'count_trips']
    df_frequent_trajectories = pd.DataFrame(columns=frequent_trajectories_columns)
    
    if df_gps_logs is not None and df_gps_logs.shape[0] > 0:
               
        # Here we transform the trajectory details into moves to perform the clustering
        df_moves = TrajectoryClustering.get_gps_logs_moves(df_gps_logs)
        # Here we get the staring and ending locations of each trajectory in the OD
        moves_start_end_locations = TrajectoryClustering.get_moves_start_end_locations(df_moves)
        if len(moves_start_end_locations) > 0:
            # Perform the clustering over these locations
            clusters_db_means = TrajectoryClustering.get_moves_start_end_locations_clusters_db_means(moves_start_end_locations)
            if clusters_db_means.clusters_labels is not None and len(set(clusters_db_means.clusters_labels)) > 0:
                # Assign the clusters to the locations
                df_clusters = TrajectoryClustering.get_df_clusters(moves_start_end_locations, clusters_db_means)
                # Assign the clusters to the moves
                df_moves = TrajectoryClustering.set_clusters_ids_to_moves(df_clusters, df_moves)
                # Filter only the interesting clusters
                df_clusters = TrajectoryClustering.filter_clusters(df_clusters)
                # Get the top values for the moves
                df_max_moves = TrajectoryClustering.get_max_moves(df_moves)
                # Get the top cluster pairs
                top_n_clusters_pairs = TrajectoryClustering.get_top_n_clusters_pairs(df_max_moves)

                # Here we are looping through the top n pairs but we can keep it simple and take just the first cluster pair
                for cluster_pair in top_n_clusters_pairs:
                    # load one group of trips, trajectories with the same start and end location
                    cluster_start = cluster_pair[0] 
                    cluster_end = cluster_pair[1] 

                    # Get only the trajectories connecting the OD pair
                    od_trajectories = TrajectoryClustering.get_trajectories(cluster_start, cluster_end, df_move=df_moves, df_cluster=df_clusters, df_logs=df_gps_logs)

                    # Reduce od_trajectories data points by RDP algorithm            
                    od_trajectories_reduced = [TrajectoryClustering.reduce_polyline_points_by_rdp(p, epsilon=config.reduce_polyline_points_by_rdp_eps_degrees, return_indices=False) 
                                               for p in od_trajectories]       
                    trajectories_reduced = [t for t in od_trajectories_reduced]

                    # Compute the distance matrix
                    dist_mat_reduced = TrajectoryClustering.compute_distance_matrix(trajectories_reduced)

                    # Label each of the trajectories (min_samples = 1 to put every trajectory into a cluster)
                    labels = TrajectoryClustering.clustering_by_dbscan(dist_mat_reduced, min_samples=1, eps=config.dbscan_kmeans_eps_in_degrees * 5)

                    # Get the top 1 frequent trajectory for that cluster
                    df_frequent_trajectories = TrajectoryClustering.get_representative_cluster_trajectory(day, hour, df_gps_logs.iloc[0].origin, df_gps_logs.iloc[0].dest, trajectories_reduced, labels)

    return df_frequent_trajectories


def get_gps_logs(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client):
    
    od_pairs_mat = get_ODs(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client)

    #print(f"Got the od_pairs_mat in get_gps_logs {od_pairs_mat}")
    if od_pairs_mat is not None and od_pairs_mat.shape[0] > 0:
        trajectories_ods_details = get_ODs_trajectories_details(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client)

        trajectories_ods_details['lat'] = trajectories_ods_details['lat'].astype(float)
        trajectories_ods_details['lon'] = trajectories_ods_details['lon'].astype(float)

        trajectories_ods_details['origin'] = od_pairs_mat['origin'].iloc[0]
        trajectories_ods_details['dest'] = od_pairs_mat['dest'].iloc[0]

        trajectories_ods_details = trajectories_ods_details.drop_duplicates()

        return trajectories_ods_details
    
    return None


def get_ODs(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client):
    
    sql = f"""SELECT DISTINCT origin_polygon_id, 
                              destination_polygon_id, 
                              residency_status, 
                              SUM(count_people) as count_people, 
                              SUM(count_trips) as count_trips, 
                              EXTRACT(HOUR FROM datetime) as hour, 
                              EXTRACT(DATE FROM datetime) as day    
    FROM {project_dataset}.{config.table_OD} 
    WHERE origin_polygon_id != destination_polygon_id
    AND EXTRACT(DATE FROM datetime) = DATE('{day}')
    AND EXTRACT(HOUR FROM datetime) = {hour}
    GROUP BY residency_status, origin_polygon_id, destination_polygon_id, EXTRACT(HOUR FROM datetime), EXTRACT(DATE FROM datetime)
    HAVING SUM(count_people) >= {min_count_people} 
    AND SUM(count_trips) >= {min_count_trips}
    AND residency_status = '{residency_status}'"""
    
    od_pairs_mat = bigquery_client.query(sql).to_dataframe()
    
    #print(sql)
    if od_pairs_mat is not None and od_pairs_mat.shape[0] > 0:
    
        od_pairs_mat['origin_lat_lon'] = od_pairs_mat.apply(lambda row: s2cell.cell_id_to_lat_lon(row.origin_polygon_id), axis=1)
        od_pairs_mat['origin_lat'], od_pairs_mat['origin_lon'] = zip(*od_pairs_mat.origin_lat_lon)    
        od_pairs_mat['dest_lat_lon'] =  od_pairs_mat.apply(lambda row: s2cell.cell_id_to_lat_lon(row.destination_polygon_id), axis=1) 
        od_pairs_mat['dest_lat'], od_pairs_mat['dest_lon'] = zip(*od_pairs_mat.dest_lat_lon)

        return od_pairs_mat
    
    return None

def get_ODs_trajectories_details(min_count_people, min_count_trips, day, hour, residency_status, project_dataset, bigquery_client):
    
    s2_level = 16
    cols = ['TRAJ_DET.trajectory_id', 'TRAJ_DET.lat', 'TRAJ_DET.lon', 'TRAJ_DET.datetime', f'TRAJ_DET.{config.USER_ID} AS user_id']
    
    query = f"""with OD as (
                SELECT DISTINCT 
                            origin_polygon_id, 
                            destination_polygon_id, 
                            residency_status, 
                            SUM(count_people) as count_people, 
                            SUM(count_trips) as count_trips, 
                            EXTRACT(HOUR FROM datetime) as hour, 
                            EXTRACT(DATE FROM datetime) as day    
                FROM {project_dataset}.{config.table_OD} 
                WHERE 
                    origin_polygon_id != destination_polygon_id
                    AND EXTRACT(DATE FROM datetime) = DATE('{day}')
                    AND EXTRACT(HOUR FROM datetime) = {hour}
                GROUP BY 
                    residency_status, 
                    origin_polygon_id, 
                    destination_polygon_id, 
                    EXTRACT(HOUR FROM datetime), 
                    EXTRACT(DATE FROM datetime)
                HAVING 
                    SUM(count_people) >= {min_count_people} 
                    AND SUM(count_trips) >= {min_count_trips}
                    AND residency_status = '{residency_status}
                )
                SELECT DISTINCT {', '.join(cols)}    
                FROM 
                    OD
                    INNER JOIN {project_dataset}.{config.table_trajectories} TRAJ 
                        ON S2_CELLIDFROMPOINT(TRAJ.start_point, {s2_level}) = OD.origin_polygon_id 
                            AND S2_CELLIDFROMPOINT(TRAJ.end_point, {s2_level}) = OD.destination_polygon_id
                    INNER JOIN {project_dataset}.{config.table_trajectory_details} AS TRAJ_DET 
                        ON TRAJ.trajectory_id = TRAJ_DET.trajectory_id
                WHERE 
                    OD.origin_polygon_id != OD.destination_polygon_id
                    AND EXTRACT(HOUR FROM TRAJ.start_time) = {hour}
                    AND DATE(TRAJ.start_time) = DATE('{day}')
                GROUP BY 
                    TRAJ_DET.lat, 
                    TRAJ_DET.lon, 
                    TRAJ_DET.trajectory_id, 
                    TRAJ_DET.datetime, 
                    TRAJ_DET.{config.USER_ID}
                ORDER BY 
                    TRAJ_DET.trajectory_id, 
                    TRAJ_DET.datetime"""
    
    #print(query)
   
    results = bigquery_client.query(query).to_dataframe()
    
    results['lat'] = results['lat'].astype(float)
    results['lon'] = results['lon'].astype(float)
    
    return results