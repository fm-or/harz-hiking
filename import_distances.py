from neo4j.v1 import GraphDatabase
import requests
import time

origin = {"lon": 10.337343, "lat": 51.803052}
radius = 40000

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    haltestellen = []
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:Haltestelle) " \
            "WHERE distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN ID(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name" \
            "".format(origin_lon=origin["lon"], origin_lat=origin["lat"], radius=radius)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        haltestellen.append((id, lat, lon, name))

    stempelstellen = []
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:Stempelstelle) " \
            "WHERE distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN n.id AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name " \
            "".format(origin_lon=origin["lon"], origin_lat=origin["lat"], radius=radius)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        stempelstellen.append((id, lat, lon, name))

    print(len(haltestellen), "Haltestellen und", len(stempelstellen), "Stempelstellen")

    stempelstellen_values = [[lon, lat] for (id, lat, lon, name) in stempelstellen]
    haltestellen_values = [[lon, lat] for (id, lat, lon, name) in haltestellen]

    # Haltestellen -> Stempelstellen #
    haltestellen_min = 0
    haltestellen_step = 50
    haltestellen_max = haltestellen_min+haltestellen_step
    while haltestellen_max < len(haltestellen):
        stempelstellen_min = 0
        stempelstellen_step = 50
        stempelstellen_max = stempelstellen_min+stempelstellen_step
        while stempelstellen_max < len(stempelstellen):
            # print(haltestellen_values[haltestellen_min:haltestellen_max])
            # print(stempelstellen_values[stempelstellen_min:stempelstellen_max])
            haltestellen_subset = haltestellen[haltestellen_min:haltestellen_max]
            stempelstellen_subset = stempelstellen[stempelstellen_min:stempelstellen_max]

            locations = haltestellen_values[haltestellen_min:haltestellen_max] + stempelstellen_values[stempelstellen_min:stempelstellen_max]
            # print(locations)

            body = {"locations": locations,
                    "destinations": list(range(haltestellen_step)),
                    "metrics": ["distance"]}  # "duration"

            headers = {
                'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
                'Authorization': '5b3ce3597851110001cf6248d990de15874844ddac1347c5541fbcdd'
            }
            # start = time.time()
            call = requests.post('https://api.openrouteservice.org/v2/matrix/foot-hiking', json=body, headers=headers)
            # print(round(time.time()-start, 2), "Sekunden")

            distances = call.json()["distances"]
            # print(distances)
            for stempelstelle_id, distances_to_stempelstelle in enumerate(distances[haltestellen_step:]):
                for haltestelle_id, distance in enumerate(distances_to_stempelstelle):
                    query = "MATCH (h:Haltestelle) WHERE ID(h) = {haltestelle_id} " \
                            "MATCH (s:Stempelstelle) WHERE s.id = {stempelstelle_id} " \
                            "WITH h, s WHERE NOT (h)-[:TO]->(s) " \
                            "CREATE (h)-[:TO {{distance: {distance}}}]->(s) " \
                            "".format(haltestelle_id=haltestellen_subset[haltestelle_id][0],
                                      stempelstelle_id=stempelstellen_subset[stempelstelle_id][0],
                                      distance=distance)
                    session.run(query)
            print("Entfernung zu", stempelstellen_max, "Stempelstellen importiert.")
            stempelstellen_min += stempelstellen_step-5
            stempelstellen_max = stempelstellen_min+stempelstellen_step
        print("Entfernung von", haltestellen_max, "Haltestellen importiert.")
        haltestellen_min += haltestellen_step-5
        haltestellen_max = haltestellen_min+haltestellen_step

    exit()

    # Stempelstellen <-> Origin #
    query = "MATCH (n:Startpunkt) RETURN COUNT(n)"
    results = session.run(query)
    origin_number = results.__iter__().__next__()[0]
    print(origin_number)
    if origin_number == 0:
        query = "CREATE (n:Startpunkt {{longitude: {longitude}, latitude: {latitude}, name: 'Wohnung'}})" \
                "".format(longitude=origin["lon"], latitude=origin["lat"])
        session.run(query)
        print("Startpunkt eingefügt")

    body = {"locations": [[origin["lon"], origin["lat"]]] + stempelstellen_values,
            "destinations": [0],
            "metrics": ["distance"]}  # "duration"

    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Authorization': '5b3ce3597851110001cf6248d990de15874844ddac1347c5541fbcdd'
    }
    start = time.time()
    call = requests.post('https://api.openrouteservice.org/v2/matrix/foot-hiking', json=body, headers=headers)
    print(round(time.time()-start, 2), "Sekunden")

    distances = call.json()["distances"]
    # print(distances)
    for stempelstelle_id, distances_to_stempelstelle in enumerate(distances[1:]):
        distance = distances_to_stempelstelle[0]
        query = "MATCH (h:Startpunkt) " \
                "MATCH (s:Stempelstelle) WHERE s.id = {stempelstelle_id} " \
                "WITH h, s WHERE NOT (h)-[:TO]->(s) " \
                "CREATE (h)-[:TO {{distance: {distance}}}]->(s) " \
                "CREATE (h)<-[:TO {{distance: {distance}}}]-(s) " \
                "".format(stempelstelle_id=stempelstellen[stempelstelle_id][0],
                          distance=distance)
        session.run(query)

    # Stempelstellen <-> Stempelstellen #
    body = {"locations": stempelstellen_values,
            "metrics": ["distance"]}  # "duration"

    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Authorization': '5b3ce3597851110001cf6248d990de15874844ddac1347c5541fbcdd'
    }
    start = time.time()
    call = requests.post('https://api.openrouteservice.org/v2/matrix/foot-hiking', json=body, headers=headers)
    print(round(time.time()-start, 2), "Sekunden")

    distances = call.json()["distances"]
    # print(distances)
    for stempelstelle_1_id, distances_to_stempelstelle in enumerate(distances):
        for stempelstelle_2_id, distance in enumerate(distances_to_stempelstelle):
            if stempelstelle_1_id != stempelstelle_2_id:
                query = "MATCH (s1:Stempelstelle) WHERE s1.id = {stempelstelle_1_id} " \
                        "MATCH (s2:Stempelstelle) WHERE s2.id = {stempelstelle_2_id} " \
                        "WITH s1, s2 WHERE NOT (s1)-[:TO]->(s2) " \
                        "CREATE (s1)-[:TO {{distance: {distance}}}]->(s2) " \
                        "".format(stempelstelle_1_id=stempelstellen[stempelstelle_1_id][0],
                                  stempelstelle_2_id=stempelstellen[stempelstelle_2_id][0],
                                  distance=distance)
                session.run(query)
    print("Entfernung zwischen", len(stempelstellen), "Stempelstellen importiert.")
