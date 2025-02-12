from neo4j import GraphDatabase
import requests
import time
import osmnx as ox
import networkx as nx


origin = {"lon": 10.337343, "lat": 51.803052}
radius = 40000
import_haltestellen_stempelstellen = True
import_stempelstellen_origin = True
import_stempelstellen_stempelstellen = True

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    haltestellen = []
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
        haltestellen.append((id, lat, lon, name))

    stempelstellen = []
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
        stempelstellen.append((id, lat, lon, name))

    print(len(haltestellen), "Haltestellen und", len(stempelstellen), "Stempelstellen")
    print("Importiere Geodaten")
    ox.settings.log_console = False
    G = ox.graph_from_point((origin["lat"], origin["lon"]), dist=radius, dist_type='network', network_type='walk', simplify=False)
    print("Finde haltestellen nodes")
    haltestellen_nodes = ox.distance.nearest_nodes(G, [lon for (id, lat, lon, name) in haltestellen], [lat for (id, lat, lon, name) in haltestellen])
    print("Finde stempelstellen nodes")
    stempelstellen_nodes = ox.distance.nearest_nodes(G, [lon for (id, lat, lon, name) in stempelstellen], [lat for (id, lat, lon, name) in stempelstellen])

    if import_haltestellen_stempelstellen:
        print("Berechne kürzeste Wege")
        haltestellen_stempelstellen_orig = [haltestellen_nodes[i] for i in range(len(haltestellen)) for j in range(len(stempelstellen))]
        haltestellen_stempelstellen_dest = [stempelstellen_nodes[j] for i in range(len(haltestellen)) for j in range(len(stempelstellen))]
        haltestellen_stempelstellen_routes = ox.routing.shortest_path(G, orig=haltestellen_stempelstellen_orig, dest=haltestellen_stempelstellen_dest, weight='length', cpus=2)
        for i in range(len(haltestellen)):
            for j in range(len(stempelstellen)):
                route = haltestellen_stempelstellen_routes[i*len(stempelstellen)+j]
                if len(route) == 1:
                    distance = 0
                else:
                    gdf = ox.routing.route_to_gdf(G, route, weight='length')
                    distance = gdf['length'].sum()
                query = "MATCH (h:BusStop) WHERE elementId(h) = '{haltestelle_id}' " \
                        "MATCH (s:StampPoint) WHERE elementId(s) = '{stempelstelle_id}' " \
                        "WITH h, s WHERE NOT (h)-[:TO]->(s) " \
                        "CREATE (h)-[:TO {{distance: {distance}}}]->(s) " \
                        "".format(haltestelle_id=haltestellen[i][0],
                                    stempelstelle_id=stempelstellen[j][0],
                                    distance=distance)
                session.run(query)
        print(f"Entfernung von {len(haltestellen)} Haltestellen zu {len(stempelstellen)} Stempelstellen importiert.")

    if import_stempelstellen_stempelstellen:
        stempelstellen_stempelstellen_orig = [stempelstellen_nodes[i] for i in range(len(stempelstellen)) for j in range(len(stempelstellen))]
        stempelstellen_stempelstellen_dest = [stempelstellen_nodes[j] for i in range(len(stempelstellen)) for j in range(len(stempelstellen))]
        stempelstellen_stempelstellen_routes = ox.routing.shortest_path(G, orig=stempelstellen_stempelstellen_orig, dest=stempelstellen_stempelstellen_dest, weight='length', cpus=2)
        for i in range(len(stempelstellen)):
            for j in range(len(stempelstellen)):
                route = stempelstellen_stempelstellen_routes[i*len(stempelstellen)+j]
                if len(route) == 1:
                    distance = 0
                else:
                    gdf = ox.routing.route_to_gdf(G, route, weight='length')
                    distance = gdf['length'].sum()
                if stempelstellen[i][0] == '4:8f59340a-713c-402f-ab89-28461c261d46:517' and stempelstellen[j][0] == '4:8f59340a-713c-402f-ab89-28461c261d46:516':
                    print("FOUND IT");
                query = "MATCH (s1:StampPoint) WHERE elementId(s1) = '{stempelstelle_1_id}' " \
                        "MATCH (s2:StampPoint) WHERE elementId(s2) = '{stempelstelle_2_id}' " \
                        "WITH s1, s2 WHERE NOT (s1)-[:TO]->(s2) " \
                        "CREATE (s1)-[:TO {{distance: {distance}}}]->(s2) " \
                        "".format(stempelstelle_1_id=stempelstellen[i][0],
                                    stempelstelle_2_id=stempelstellen[j][0],
                                    distance=distance)
                session.run(query)
        print(f"Entfernung zwischen {len(stempelstellen)} Stempelstellen importiert.")

    if import_stempelstellen_origin:
        query = "MATCH (n:start) RETURN COUNT(n)"
        results = session.run(query)
        origin_number = results.__iter__().__next__()[0]
        if origin_number == 0:
            query = "CREATE (n:start {{longitude: {longitude}, latitude: {latitude}, name: 'Flat'}})" \
                    "".format(longitude=origin["lon"], latitude=origin["lat"])
            session.run(query)
            print("Startpunkt eingefügt")

        origin_node = ox.distance.nearest_nodes(G, origin['lon'], origin['lat'])
        origin_stempelstellen_orig = [origin_node] * len(stempelstellen)
        origin_stempelstellen_dest = stempelstellen_nodes
        origin_stempelstellen_routes = ox.routing.shortest_path(G, orig=origin_stempelstellen_orig, dest=origin_stempelstellen_dest, weight='length', cpus=2)
        for i in range(len(stempelstellen)):
            route = origin_stempelstellen_routes[i]
            if len(route) == 1:
                distance = 0
            else:
                gdf = ox.routing.route_to_gdf(G, route, weight='length')
                distance = gdf['length'].sum()
            query = "MATCH (h:start) " \
                    "MATCH (s:StampPoint) WHERE elementId(s) = '{stempelstelle_id}' " \
                    "WITH h, s WHERE NOT (h)-[:TO]->(s) " \
                    "CREATE (h)-[:TO {{distance: {distance}}}]->(s) " \
                    "CREATE (h)<-[:TO {{distance: {distance}}}]-(s) " \
                    "".format(stempelstelle_id=stempelstellen[i][0],
                              distance=distance)
            session.run(query)
        print(f"Entfernung vom Ursprung zu {len(stempelstellen)} Stempelstellen importiert.")
