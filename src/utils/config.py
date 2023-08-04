schema_dictionary = {"geography": "GEOGRAPHY",
                     "float": "FLOAT",
                     "string": "STRING"}


data_columns = ["imsi", "rec_timestamp", "lat", "lon", "mcc", "ue_type_vendor_name", "ue_type_model_name", "day_part"]

table_trajectories = "trajectories_demo" 
table_trajectory_details = "trajectory_details_demo" 
table_stay_points = "stay_points_demo" 

unfinished_trajectories_table = "unfinished_trajectories_table"
table_OD = "OD_matrix_16_demo"
table_OD_totals = "OD_matrix_16_totals_demo"
table_users = "user_profile_demo"
table_polygons = "polygons"
table_id_frequent_trajectories_BB8 = "frequent_trajectories"
table_mode_of_transport = "mode_of_transport_demo"
table_user_night_location = "night_stay_s2cell_demo"
table_user_work_location = "work_place_s2cell_demo"
table_user_professional = "user_professional_status_profile_demo"
table_mcc = "table_mcc"
table_night_stay_type = "night_stay_type_demo"


mode_of_transport_labels = ["stationary", "undefined",
                            "train", "bus", "car", "foot", "bike"]

start_datetime_demo = "2023-06-20 00:00:00"
end_datetime_demo = "2023-06-27 00:00:00"
demo_number_hours = 168  #  number of hours in a week
demo_number_days = 7

USER_ID = "imsi"

TRAJECTORY_CONVERT_TIME_FORMAT = "%Y%m%d%H%M%S"
NEW_TRAJECTORY_TIME_THRESHOLD = 1800  # 1800 seconds equals to 30 minutes
NEW_TRAJECTORY_DISTANCE_THRESHOLD = 1000 # 1 km

STAY_POINT_TIME_THRESHOLD = 900 # 15 minutes

THRESHOLD_VISIT_INSIDE = 120  # 2 minutes
THRESHOLD_VISIT_OUTSIDE = 600  # 10 minutes: people have to be outside the area for 10 mins to consider a new visit (still a bit low, but should remove random jumps)

COMPRESSION_THRESHOLD = 25 # meters
VELOCITY_THRESHOLD = 35 # meters per second. perhaps a bit high?

s2_level = 16

friendly_weekday = {1: "Sunday", 2: "Monday", 3: "Tuesday", 4: "Wednesday", 5: "Thursday", 6: "Friday", 7: "Saturday"}
friendly_weekday_reverse = {"Sunday":1, "Monday":2, "Tuesday": 3, "Wednesday":4, "Thursday":5, "Friday":6, "Saturday":7}

modes_of_transport = {0: "train", 1: 'car', 2: 'walk', 3: 'bus', 4: 'bike'} 

mode_of_transport_model = "building_blocks/self_updating_MLP.pkl"

"""
Frequent trajectories settings
"""
reduce_polyline_points_by_rdp_eps_degrees = 0.0005
reduce_polyline_points_by_rdp_eps_meters = 50

freq_traj_num_clusters_to_analyze = 6

dbscan_kmeans_eps_in_degrees = 0.0005
dbscan_kmeans_min_samples = 10

H3_res = 12
"""
Frequent trajectories settings
"""

"""
Night stay settings
"""

min_night_stay_time = 120 # 2 hours