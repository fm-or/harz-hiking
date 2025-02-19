from model.GraphData import GraphData


graph_data = GraphData(
    neo4j_uri = "bolt://localhost:7687",
    neo4j_user = "neo4j",
    neo4j_password = "12345678")
graph_data.import_data(
    stampPoint_gpx_filename = 'HWN2024.gpx',
    max_distance_m = 40000,
    force_update = False,
    output = True)

# problem configuration

# solve

# output solution