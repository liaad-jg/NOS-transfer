import utils.config as config
from sklearn.preprocessing import StandardScaler

def get_trajectory_stats(time_start, time_end, project_dataset, client):
    
    query = f"""SELECT * 
                FROM {project_dataset}.{config.table_trajectory_details}
                WHERE trajectory_id IN (SELECT trajectory_id 
                                      FROM {project_dataset}.{config.table_trajectories} 
                                      WHERE n_points >= 4
                                            AND total_distance >= 1000
                                            AND total_time >= 300
                                            AND start_time >= '{time_start}' AND start_time < '{time_end}')"""

    trajectory_details = client.query(query).to_dataframe()
    trajectory_stats = trajectory_details.groupby("trajectory_id").agg(v_ave = ("velocity", "mean"),
                                                                       v_med = ("velocity", "median"),
                                                                       v_max = ("velocity", "max"),
                                                                       v_std = ("velocity", "std"),
                                                                       a_ave = ("acceleration", "mean"),
                                                                       a_med = ("acceleration", "median"),
                                                                       a_max = ("acceleration", "max"),
                                                                       a_std = ("acceleration", "std")).reset_index()
    
    return trajectory_stats


def get_stationary_trajectories(day, project_dataset, client):
    
    query = f"""SELECT DISTINCT trajectory_id 
                FROM {project_dataset}.{config.table_trajectories} 
                WHERE DATE(start_time) = DATE('{day}')
                AND total_distance = 0"""
    
    stationary_trajs = client.query(query).to_dataframe()
    stationary_trajs["mode_of_transport"] = "stationary"
    return stationary_trajs


def get_undefined_trajectories(day, project_dataset, client):
    
    query = f"""SELECT DISTINCT trajectory_id 
                FROM {project_dataset}.{config.table_trajectories} 
                WHERE DATE(start_time) = DATE('{day}')
                AND (n_points < 4 OR total_distance < 1000 OR total_time < 300)"""
    
    undefined_trajs = client.query(query).to_dataframe()
    undefined_trajs["mode_of_transport"] = "undefined"
    return undefined_trajs



def map_predictions_to_label(predictions):
    
    return list(map(lambda x: config.modes_of_transport[x], predictions))


def assign_transport_mode_label(time_start, time_end, classifier, project_dataset, client):
    
    trajectory_stats = get_trajectory_stats(time_start, time_end, project_dataset, client)
    if len(trajectory_stats) == 0:
        return
    features = trajectory_stats.drop(columns = ["trajectory_id"]).values
    
    predictions = classifier.predict(StandardScaler().fit_transform(features))
    transport_labels = map_predictions_to_label(predictions)
    
    trajectory_stats["mode_of_transport"] = transport_labels
    return trajectory_stats.loc[:, ["trajectory_id", "mode_of_transport"]]


def assign_transport_mode_label_RF(time_start, time_end, classifier, project_dataset, client):
    
    trajectory_stats = get_trajectory_stats(time_start, time_end, project_dataset, client)
    if len(trajectory_stats) == 0:
        return
    features = trajectory_stats.drop(columns = ["trajectory_id"]).values
    
    predictions = classifier.predict(features)
    transport_labels = map_predictions_to_label(predictions)
    
    trajectory_stats["mode_of_transport"] = transport_labels
    return trajectory_stats.loc[:, ["trajectory_id", "mode_of_transport"]]