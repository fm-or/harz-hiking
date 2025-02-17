from model.Importer import Importer


neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "12345678"
force_update = False
importer = Importer(neo4j_uri, neo4j_user, neo4j_password)

# import stamp points
stampPoint_gpx_filename = 'HWN2024.gpx'
new_stampPoint_count = importer.import_stampPoints(stampPoint_gpx_filename, force_update=force_update)
if new_stampPoint_count > 0:
    print(f"Imported {new_stampPoint_count} new stamp points.")

# import bus stops/parking lots
new_busStop_count = importer.import_busStops(ignore_radius=500.0, force_update=force_update)
if new_busStop_count > 0:
    print(f"Imported {new_busStop_count} new bus stops lots.")

new_parkingLot_count = importer.import_parkingLots(ignore_radius=500.0, force_update=force_update)
if new_parkingLot_count > 0:
    print(f"Imported {new_parkingLot_count} new parking lots.")

# import home

# import distances
new_distances_count = importer.import_distances(max_distance_m=5000.0)

# problem configuration

# solve

# output solution