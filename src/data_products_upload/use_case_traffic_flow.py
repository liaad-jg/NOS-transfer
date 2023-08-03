import google.auth
from google.cloud import bigquery
import utils.config as config
from building_blocks.BB5 import large_traffic_flow_section, large_traffic_flow, large_traffic_flow_freguesia
from utils.bigquery_utils import upload_table_bq


credentials, project = google.auth.default()
client = bigquery.Client(credentials=credentials)
project_dataset = f"{project}.rw_data_west1"

output_dataset = f"{project}.data_products"
output_table = f"{output_dataset}.traffic_flow"
output_polygons = client.query(f"SELECT * FROM {output_dataset}.polygons WHERE cocode = '0303' OR cocode = '1313'").to_dataframe()

functions_dict = {"Secção": large_traffic_flow_section,
                  "Celula": large_traffic_flow,
                  "Freguesia": large_traffic_flow_freguesia}

cols_dict = {"Secção": "sccode",
             "Celula": "s2_id",
             "Freguesia": "frcode"}

cocodes = ["0303", "1313"]

def upload_traffic_flow(cocode, polygon_description, client, projec_dataset):

    pol_cover = output_polygons[(output_polygons.polygon_description == polygon_description) &
                                (output_polygons.cocode == cocode)].polygon_id.unique()
    
    traffic_flow = functions_dict[polygon_description](None, config.start_datetime_demo,
                                                       config.end_datetime_demo, client, project_dataset,
                                                       polygon_cover = pol_cover)
    
    traffic_flow["datetime"] = traffic_flow[["day", "hour"]].apply(lambda x: f"{x['day']} {x['hour']:02d}:00:00", axis=1)
    traffic_flow = traffic_flow.rename(columns = {cols_dict[polygon_description]: "polygon_id",
                                                  "spatial_significance": "significance_spatial",
                                                  "temporal_significance": "significance_temporal"})
    traffic_flow.fillna(0, inplace=True)
    traffic_flow.polygon_id = traffic_flow.polygon_id.astype(str)
    upload_table_bq(traffic_flow, output_table, schema = {"polygon_id": "string"}, client = client)
    
for cocode, polygon_description in zip(cocodes, ["Celula", "Secção", "Freguesia"]):
    upload_traffic_flow(cocode, polygon_description, client, project_dataset)