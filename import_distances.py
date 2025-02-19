from neo4j import GraphDatabase
import requests
from time import perf_counter
import osmnx as ox
import networkx as nx


origin = {"lon": 10.337343, "lat": 51.803052}
radius = 20000
import_bus_stops_stamp_points = True
import_stamp_points_origin = True
import_stamp_points_stamp_points = True
threads=1

start_time = perf_counter()
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    bus_stops = []
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:BusStop) " \
            "WHERE point.distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name ORDER BY id DESC" \
            "".format(origin_lon=origin["lon"], origin_lat=origin["lat"], radius=radius)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        bus_stops.append((id, lat, lon, name))

    stamp_points = []
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:StampPoint) " \
            "WHERE point.distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name ORDER BY id ASC" \
            "".format(origin_lon=origin["lon"], origin_lat=origin["lat"], radius=radius)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        stamp_points.append((id, lat, lon, name))

    print(len(bus_stops), "bus_stops und", len(stamp_points), "stamp_points")
    print("Importiere Geodaten")
    ox.settings.log_console = False
    G = ox.graph_from_point((origin["lat"], origin["lon"]), dist=radius, dist_type='network', network_type='walk', simplify=False)
    print("Finde bus_stops nodes")
    bus_stops_nodes = ox.distance.nearest_nodes(G, [lon for (id, lat, lon, name) in bus_stops], [lat for (id, lat, lon, name) in bus_stops])
    print("Finde stamp_point nodes")
    stamp_points_nodes = ox.distance.nearest_nodes(G, [lon for (id, lat, lon, name) in stamp_points], [lat for (id, lat, lon, name) in stamp_points])

    if import_bus_stops_stamp_points:
        print("Berechne kürzeste Wege")
        bus_stops_stamp_points_orig = [bus_stops_nodes[i] for i in range(len(bus_stops)) for j in range(len(stamp_points))]
        bus_stops_stamp_points_dest = [stamp_points_nodes[j] for i in range(len(bus_stops)) for j in range(len(stamp_points))]
        bus_stops_stamp_points_routes = ox.routing.shortest_path(G, orig=bus_stops_stamp_points_orig, dest=bus_stops_stamp_points_dest, weight='length', cpus=threads)
        print("Speichere Distanzen")
        for i in range(len(bus_stops)):
            for j in range(len(stamp_points)):
                route = bus_stops_stamp_points_routes[i*len(stamp_points)+j]
                if len(route) == 1:
                    distance = 0
                else:
                    gdf = ox.routing.route_to_gdf(G, route, weight='length')
                    distance = gdf['length'].sum()
                query = "MATCH (h:BusStop) WHERE elementId(h) = '{bus_stop_id}' " \
                        "MATCH (s:StampPoint) WHERE elementId(s) = '{stamp_point_id}' " \
                        "WITH h, s WHERE NOT (h)-[:TO]->(s) " \
                        "CREATE (h)-[:TO {{distance: {distance}}}]->(s) " \
                        "".format(bus_stop_id=bus_stops[i][0],
                                    stamp_point_id=stamp_points[j][0],
                                    distance=distance)
                session.run(query)
        print(f"Entfernung von {len(bus_stops)} bus_stops zu {len(stamp_points)} stamp_point importiert.")

    if import_stamp_points_stamp_points:
        print("Berechne kürzeste Wege")
        stamp_points_stamp_points_orig = [stamp_points_nodes[i] for i in range(len(stamp_points)) for j in range(len(stamp_points))]
        stamp_points_stamp_points_dest = [stamp_points_nodes[j] for i in range(len(stamp_points)) for j in range(len(stamp_points))]
        stamp_points_stamp_points_routes = ox.routing.shortest_path(G, orig=stamp_points_stamp_points_orig, dest=stamp_points_stamp_points_dest, weight='length', cpus=threads)
        print("Speichere Distanzen")
        for i in range(len(stamp_points)):
            for j in range(len(stamp_points)):
                route = stamp_points_stamp_points_routes[i*len(stamp_points)+j]
                if len(route) == 1:
                    distance = 0
                else:
                    gdf = ox.routing.route_to_gdf(G, route, weight='length')
                    distance = gdf['length'].sum()
                query = "MATCH (s1:StampPoint) WHERE elementId(s1) = '{stamp_point_1_id}' " \
                        "MATCH (s2:StampPoint) WHERE elementId(s2) = '{stamp_point_2_id}' " \
                        "WITH s1, s2 WHERE NOT (s1)-[:TO]->(s2) " \
                        "CREATE (s1)-[:TO {{distance: {distance}}}]->(s2) " \
                        "".format(stamp_point_1_id=stamp_points[i][0],
                                    stamp_point_2_id=stamp_points[j][0],
                                    distance=distance)
                session.run(query)
        print(f"Entfernung zwischen {len(stamp_points)} stamp_points importiert.")

    if import_stamp_points_origin:
        query = "MATCH (n:start) RETURN COUNT(n)"
        results = session.run(query)
        origin_number = results.__iter__().__next__()[0]
        if origin_number == 0:
            query = "CREATE (n:start {{longitude: {longitude}, latitude: {latitude}, name: 'Flat'}})" \
                    "".format(longitude=origin["lon"], latitude=origin["lat"])
            session.run(query)
            print("Startpunkt eingefügt")

        print("Berechne kürzeste Wege")
        origin_node = ox.distance.nearest_nodes(G, origin['lon'], origin['lat'])
        origin_stamp_points_orig = [origin_node] * len(stamp_points)
        origin_stamp_points_dest = stamp_points_nodes
        origin_stamp_points_routes = ox.routing.shortest_path(G, orig=origin_stamp_points_orig, dest=origin_stamp_points_dest, weight='length', cpus=threads)
        print("Speichere Distanzen")
        for i in range(len(stamp_points)):
            route = origin_stamp_points_routes[i]
            if len(route) == 1:
                distance = 0
            else:
                gdf = ox.routing.route_to_gdf(G, route, weight='length')
                distance = gdf['length'].sum()
            query = "MATCH (h:start) " \
                    "MATCH (s:StampPoint) WHERE elementId(s) = '{stamp_point_id}' " \
                    "WITH h, s WHERE NOT (h)-[:TO]->(s) " \
                    "CREATE (h)-[:TO {{distance: {distance}}}]->(s) " \
                    "CREATE (h)<-[:TO {{distance: {distance}}}]-(s) " \
                    "".format(stamp_point_id=stamp_points[i][0],
                              distance=distance)
            session.run(query)
        print(f"Entfernung vom Ursprung zu {len(stamp_points)} stamp_points importiert.")
print("Laufzeit:", perf_counter()-start_time)