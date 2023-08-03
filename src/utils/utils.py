from shapely.geometry import Polygon, mapping
import numpy as np
from s2sphere import CellId, LatLng, Cell
import s2cell
import utils.config as config
import pandas as pd
import h3

def geo_to_h3(row, lat_column, lon_column):
    return h3.geo_to_h3(lat=row[lat_column], lng=row[lon_column], resolution=config.H3_res)


def swap_coordinates(tup):
    '''
    Reverses the order of the elements in a tuple. Useful to swap the order of coordinates.
    '''
    return (tup[1], tup[0])


def check_coordinates_order(polygon):
    '''
    BigQuery is expecting coordinates to come in format (lng, lat). Portugal is around -9, -8 longitude, 37 to 42 latitude. Madeira is -16/-17 longitude, 32 latitude. Azores is 36-40 latitude, -23 to -31 longitude. We just need to check if the first coordinate is negative. If not, we need to swap the coordinates.
    
    Input is supposed to be a list of coordinates, polygon = [[lat_1, lon_1], ...]. Due to MultiPolygons, we don't know how many nested lists, so we call this recursively until we reach a number
    '''
    if isinstance(polygon[0], list) or isinstance(polygon[0], tuple):
        return check_coordinates_order(polygon[0])
    return polygon[0] > 0


def get_reverse_polygon(polygon):
    
    polygon_reverse = mapping(polygon)
    poly_reverse_coordinates = polygon_reverse['coordinates'][0]

    poly_reverse = [[coords[1], coords[0]] for coords in poly_reverse_coordinates]
    return Polygon(poly_reverse)


def get_square_polygon_from_coordinates(longitude, latitude, area):
        
    center = [latitude, longitude]
    
    area_degrees = area / (6371000**2 * (1/111.32)**2)
    side_length = np.sqrt(area_degrees)
    
    corner1 = (center[0] - side_length/2, center[1] - side_length/2)
    corner2 = (center[0] + side_length/2, center[1] + side_length/2)
    
    poly = [[corner1[1], corner1[0]],
            [corner1[1], corner2[0]],
            [corner2[1], corner2[0]],
            [corner2[1], corner1[0]],
            [corner1[1], corner1[0]]]
    
    return Polygon(poly)



def polygon_to_s2list(polygon, bq_client):
    
    if isinstance(polygon, Polygon):
        polygon = mapping(polygon)["coordinates"][0]
    
    if check_coordinates_order(polygon):
        polygon = list(map(swap_coordinates, polygon))
        
    polygon = Polygon(polygon)
    
    sql = f"""WITH s2array AS (
WITH polygon AS (
SELECT '{polygon}' AS p
)
SELECT S2_COVERINGCELLIDS(ST_GEOGFROMTEXT(p), min_level => {config.s2_level}, max_level => {config.s2_level}, max_cells => 1000000) AS s2cells 
FROM polygon
)
SELECT cell_ids FROM s2array, UNNEST(s2array.s2cells) as cell_ids"""

    polygon_s2_cover = bq_client.query(sql).to_dataframe()
    
    return polygon_s2_cover.cell_ids.values


def list_polygon_to_s2list(list_of_polygons, bq_client):
    
    s2ids = np.array([])
    for polygon in list_of_polygons:
        s2ids = np.union1d(s2ids, polygon_to_s2list(polygon, bq_client))
    return s2ids



def polygon_to_section(polygon, bq_client, project_dataset):
    
    if isinstance(polygon, Polygon):
        polygon = mapping(polygon)["coordinates"][0]
    
    if check_coordinates_order(polygon):
        polygon = list(map(swap_coordinates, polygon))
        
    polygon = Polygon(polygon)
    
    sql = f"""WITH s2array AS (
                  WITH polygon AS (
                      SELECT '{polygon}' AS p
                  )
                  SELECT S2_COVERINGCELLIDS(ST_GEOGFROMTEXT(p), 
                                                 min_level => {config.s2_level}, 
                                                 max_level => {config.s2_level}, 
                                                 max_cells => 1000000) AS s2cells 
                  FROM polygon
              ),
              s2cells AS (
                  SELECT CAST(cell_ids AS STRING) AS cell_ids
                  FROM s2array, UNNEST(s2array.s2cells) as cell_ids
              ),
              scc_mapper AS (
                  SELECT DISTINCT s2code, sccode
                  FROM {project_dataset}.{config.table_polygons}
                  WHERE polygon_description = "Celula"
              )
              SELECT DISTINCT scc_mapper.sccode
              FROM s2cells INNER JOIN scc_mapper ON s2cells.cell_ids = scc_mapper.s2code
            """

    polygon_section_cover = bq_client.query(sql).to_dataframe()
    
    return polygon_section_cover.sccode.values


def list_polygon_to_section(list_of_polygons, bq_client, project_dataset):
    
    sections = np.array([])
    for polygon in list_of_polygons:
        sections = np.union1d(sections, polygon_to_section(polygon, bq_client, project_dataset))
    return sections


def polygon_to_freguesia(polygon, bq_client, project_dataset):
    
    if isinstance(polygon, Polygon):
        polygon = mapping(polygon)["coordinates"][0]
    
    if check_coordinates_order(polygon):
        polygon = list(map(swap_coordinates, polygon))
        
    polygon = Polygon(polygon)
    
    sql = f"""WITH s2array AS (
                  WITH polygon AS (
                      SELECT '{polygon}' AS p
                  )
                  SELECT S2_COVERINGCELLIDS(ST_GEOGFROMTEXT(p), 
                                                 min_level => {config.s2_level}, 
                                                 max_level => {config.s2_level}, 
                                                 max_cells => 1000000) AS s2cells
                  FROM polygon
              ),
              s2cells AS (
                  SELECT CAST(cell_ids AS STRING) AS cell_ids 
                  FROM s2array, UNNEST(s2array.s2cells) as cell_ids
              ),
              frc_mapper AS (
                  SELECT DISTINCT s2code, frcode
                  FROM {project_dataset}.{config.table_polygons}
                  WHERE polygon_description = "Celula"
              )
              SELECT DISTINCT frc_mapper.frcode
              FROM s2cells INNER JOIN frc_mapper ON s2cells.cell_ids = frc_mapper.s2code
            """

    polygon_freguesia_cover = bq_client.query(sql).to_dataframe()
    
    return polygon_freguesia_cover.frcode.values


def list_polygon_to_freguesia(list_of_polygons, bq_client, project_dataset):
    
    freguesias = np.array([])
    for polygon in list_of_polygons:
        freguesias = np.union1d(freguesias, polygon_to_freguesia(polygon, bq_client, project_dataset))
    return freguesias



def map_to_s2level(s2_id, target_s2_level):
    return s2cell.cell_id_to_parent_cell_id(int(s2_id), target_s2_level)


def get_corners_s2(s2cell_id):
    cell = Cell(CellId(int(s2cell_id)))
    
    square = [LatLng.from_point(cell.get_vertex(i)) for i in range(4)] + [LatLng.from_point(cell.get_vertex(0))]
    return [[l.lat().degrees, l.lng().degrees] for l in square]


def get_s2_centroid(s2cell_id):
    return s2cell.cell_id_to_lat_lon(s2cell_id)


def zero_pad_results(df, target_col, hourly = True):
    
    if hourly:
        datetimes = [pd.Timestamp(config.start_datetime_demo_prev) + pd.Timedelta(hours=i) for i in range(config.demo_number_hours_prev)]
        days = [date.date() for date in datetimes]
        hours = [date.hour for date in datetimes]
        base_df =  pd.DataFrame.from_dict({"day": days, "hour": hours, "number": [0 for i in range(config.demo_number_hours_prev)]})
        aggregation_column = ["day", "hour"]
    else:
        days = [(pd.Timestamp(config.start_datetime_demo_prev.split(" ")[0]) + pd.Timedelta(days=i)).date() for i in range(config.demo_number_days)]
        base_df =  pd.DataFrame.from_dict({"date": days, "number": [0 for i in range(config.demo_number_days)]})
        aggregation_column = "date"
    
    
    merged_df = base_df.merge(df, on=aggregation_column, how="left")
    merged_df[target_col] = np.fmax(merged_df["number"], merged_df[target_col])
    
    return merged_df.drop(columns=["number"])


def zero_pad_results_single_day(df, target_col):
    
    day = str(df.datetime.iloc[0].date())
    datetimes = [pd.to_datetime(f"{day} {hour:02d}:00:00", utc=True) for hour in range(24)]
    base_df =  pd.DataFrame.from_dict({"datetime": datetimes, "number": [0 for i in range(24)]})
    
    merged_df = base_df.merge(df, on="datetime", how="left")
    merged_df[target_col] = np.fmax(merged_df["number"], merged_df[target_col])
    
    return merged_df.drop(columns=["number"])



def split_dates(start_time, end_time, granularity):
    
    splitter = pd.Timedelta(**granularity)
    days_hours = []
    current_time = pd.Timestamp(start_time)
    end_time = pd.Timestamp(end_time)
    while current_time < end_time:
        days_hours.append((str(current_time.date()), current_time.hour))
        current_time += splitter
    return days_hours


def align_user_table(polygon, user_table):
    
    column_to_use = {'Concelho': "cocode", 
                     'Celula': "sccode", 
                     'Secção': "sccode", 
                     'Freguesia': "frcode", 
                     'Subsecção': "sccode", 
                     'POI': "sccode"}
    
    target_column = column_to_use[polygon["polygon_description"]]
    
    red_user_table = user_table.loc[user_table["polygon_id"] == polygon[target_column], 
                          [config.USER_ID, "residential_status"]]
    
    red_user_table["residential_status"] = pd.Categorical(red_user_table["residential_status"], 
                                                          categories=["resident", "national tourist", 'international tourist', 
                                                                      "regular visitor", "casual visitor", "commuter"], ordered=True)
    
    return red_user_table


def align_user_table_old(polygon, user_table):
    
    column_to_use = {'Concelho': "cocode", 
                     'Celula': "sccode", 
                     'Secção': "sccode", 
                     'Freguesia': "frcode", 
                     'Subsecção': "sccode", 
                     'POI': "sccode"}
    
    target_column = column_to_use[polygon["polygon_description"]]
    
    red_user_table = user_table.loc[user_table[target_column] == polygon[target_column], 
                          [config.USER_ID, "residential_status"]]
    
    red_user_table["residential_status"] = pd.Categorical(red_user_table["residential_status"], 
                                                          categories=["resident", "national tourist", 'international tourist', 
                                                                      "regular visitor", "casual visitor", "commuter"], ordered=True)
    
    return red_user_table.sort_values(by=["residential_status"]).groupby(config.USER_ID, as_index=0).first()