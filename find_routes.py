from neo4j.v1 import GraphDatabase
from gurobipy import Model, GRB, quicksum


radius = 40000
days = 7

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    query = "MATCH (n:Startpunkt) RETURN ID(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name"
    results = session.run(query)
    result = results.__iter__().__next__()
    id = result.get("id")
    lat = result.get("lat")
    lon = result.get("lon")
    name = result.get("name")
    origin = (id, lat, lon, name)

    haltestellen = {}
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:Haltestelle) " \
            "WHERE distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN ID(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name" \
            "".format(origin_lon=origin[2], origin_lat=origin[1], radius=radius)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        haltestellen[id] = (lat, lon, name)

    stempelstellen = {}
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:Stempelstelle) " \
            "WHERE distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN ID(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name " \
            "".format(origin_lon=origin[2], origin_lat=origin[1], radius=radius)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        stempelstellen[id] = (lat, lon, name)

    distances = {}
    query = "MATCH (s)-[r:TO]->(t) " \
            "RETURN ID(s) AS from_id, r.distance AS distance, ID(t) AS to_id"
    for result in session.run(query):
        from_id = result.get("from_id")
        distance = result.get("distance")
        to_id = result.get("to_id")
        distances[from_id, to_id] = distance

    model = Model()

    n = {}
    for day in range(days):
        n[day, origin[0]] = model.addVar(vtype=GRB.BINARY)
        for haltestelle in haltestellen.keys():
            n[day, haltestelle] = model.addVar(vtype=GRB.BINARY)

    for stempelstelle in stempelstellen.keys():
        n[stempelstelle] = model.addVar(vtype=GRB.BINARY)

    e = {}
    for (from_id, to_id), distance in distances.items():
        for day in range(days):
            e[day, from_id, to_id] = model.addVar(vtype=GRB.BINARY, obj=distance)
    model.update()

    for day in range(days):
        # Jeden Tag genau einen Startknoten verlassen
        model.addConstr(quicksum(e[day, from_id, to_id] for from_id in [origin[0]] + list(haltestellen.keys()) for to_id in stempelstellen.keys()) == 1)

        # Jeden Tag den Heimatort besuchen
        model.addConstr(quicksum(e[day, from_id, origin[0]] for from_id in stempelstellen) == 1)

        # Jeden Tag durch Stempelstellen weiterlaufen
        for stempelstelle in stempelstellen.keys():
            model.addConstr(quicksum(e[day, from_id, stempelstelle] for from_id in [origin[0]] + haltestellen.keys() + stempelstellen.keys())
                            == quicksum(e[day, stempelstelle, to_id] for to_id in [origin[0]] + stempelstellen.keys()))

        # Jeden Tag den Startknoten speichern
        for from_id in [origin[0]] + haltestellen.keys():
            model.addConstr(quicksum(e[day, from_id, to_id] for to_id in stempelstellen.keys()) == n[day, from_id])

    # Insgesamt die besuchten Stempelstellen speichern
    for stempelstelle in stempelstellen.keys():
        model.addConstr(quicksum(e[day, from_id, stempelstellen] for day in range(days) for from_id in [origin[0]] + haltestellen.keys() + stempelstellen.keys()) == n[stempelstelle])

    # Mindestens 50 Stempelstellen besuchen
    model.addConstr(quicksum(n[stempelstelle] for stempelstelle in stempelstellen.keys()) >= 50)
    model.optimize()
