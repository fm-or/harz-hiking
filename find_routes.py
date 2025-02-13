from neo4j import GraphDatabase
from gurobipy import Model, GRB, quicksum
from time import perf_counter

radius_max = 15000
maximum_daily_distance = 45000
days = 3
min_stamps = 35
max_bus_days = 1
bus_radius_min = 0

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    query = "MATCH (n:start) RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name"
    results = session.run(query)
    result = results.__iter__().__next__()
    id = result.get("id")
    lat = result.get("lat")
    lon = result.get("lon")
    name = result.get("name")
    origin = (id, lat, lon, name)

    bus_stops = {}
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:BusStop) " \
            "WITH n, point.distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) AS distance " \
            "WHERE distance > {radius_min} AND distance < {radius_max} " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name" \
            "".format(origin_lon=origin[2], origin_lat=origin[1], radius_min=bus_radius_min, radius_max=radius_max)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        bus_stops[id] = (lat, lon, name)

    stamp_points = {}
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:StampPoint) " \
            "WHERE point.distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius} " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name " \
            "".format(origin_lon=origin[2], origin_lat=origin[1], radius=radius_max)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        stamp_points[id] = (lat, lon, name)

    distances = {}
    edges = {}
    reverse_edges = {}
    query = "MATCH (s)-[r:TO]->(t) " \
            "RETURN elementId(s) AS from_id, r.distance AS distance, elementId(t) AS to_id"
    for result in session.run(query):
        from_id = result.get("from_id")
        distance = result.get("distance")
        to_id = result.get("to_id")
        if (from_id == origin[0] or from_id in bus_stops or from_id in stamp_points) and (to_id == origin[0] or to_id in bus_stops or to_id in stamp_points):
            distances[from_id, to_id] = distance
            if from_id not in edges:
                edges[from_id] = []
            if to_id not in reverse_edges:
                reverse_edges[to_id] = []
            edges[from_id].append(to_id)
            reverse_edges[to_id].append(from_id)

    print("Untersuche", len(bus_stops), "bus_stops und", len(stamp_points), "stamp_points")
    start_time = perf_counter()
    model = Model()
    model.setParam("outputFlag", False)

    # Is the node (origin, bus_stop, stamp_point) visited on day?
    n = {}
    # Position of the stamp_point on the tour.
    x = {}
    for day in range(days):
        n[day, origin[0]] = model.addVar(vtype=GRB.BINARY)
        for bus_stop in bus_stops:
            n[day, bus_stop] = model.addVar(vtype=GRB.BINARY)
        for stamp_point in stamp_points:
            n[day, stamp_point] = model.addVar(vtype=GRB.BINARY)
            x[day, stamp_point] = model.addVar(vtype=GRB.CONTINUOUS, lb=0, ub=len(stamp_points))

    for stamp_point in stamp_points:
        n[stamp_point] = model.addVar(vtype=GRB.BINARY)

    e = {}
    for day in range(days):
        for from_id, to_id in distances:
            e[day, from_id, to_id] = model.addVar(vtype=GRB.BINARY, name='e'+str(day)+'_'+str(from_id)+'_'+str(to_id))
    model.update()

    for day in range(days):
        # Jeden Tag genau einen Startknoten benutzen
        model.addConstr(quicksum(n[day, from_id] for from_id in list(bus_stops.keys()) + [origin[0]]) == 1)

        # Jeden Tag die Startknoten an die Kanten linken
        for from_id in list(bus_stops.keys()) + [origin[0]]:
            model.addConstr(quicksum(e[day, from_id, to_id] for to_id in stamp_points) == n[day, from_id])

        # Keine Schleifen nur aus Stempelstellen
        for from_stamp in stamp_points:
            for to_stamp in stamp_points:
                if (from_stamp, to_stamp) in distances:
                    model.addConstr(x[day, to_stamp] >=  x[day, from_stamp] + 1 - len(stamp_points)*(1 - e[day, from_stamp, to_stamp]))

        # Jeden Tag den Heimatort besuchen
        model.addConstr(quicksum(e[day, from_id, origin[0]] for from_id in stamp_points) == 1)

        # Jeden Tag durch stamp_points weiterlaufen
        for stamp_point in stamp_points:
            model.addConstr(quicksum(e[day, from_id, stamp_point] for from_id in [origin[0]] + list(bus_stops.keys()) + list(stamp_points.keys()))
                            == quicksum(e[day, stamp_point, to_id] for to_id in [origin[0]] + list(stamp_points.keys())))

        # Jeden Tag den Startknoten speichern
        for from_id in bus_stops:
            model.addConstr(quicksum(e[day, from_id, to_id] for to_id in stamp_points) == n[day, from_id])

    # Jeden Tag die besuchten stamp_points speichern
    for day in range(days):
        for stamp_point in stamp_points:
            model.addConstr(quicksum(e[day, from_id, stamp_point] for from_id in [origin[0]] + list(bus_stops.keys()) + list(stamp_points.keys())) == n[day, stamp_point])

    # Insgesamt die besuchten stamp_points speichern
    for stamp_point in stamp_points:
        model.addConstr(quicksum(n[day, stamp_point] for day in range(days)) == n[stamp_point])

    # Mindestens 50 stamp_points besuchen
    model.addConstr(quicksum(n[stamp_point] for stamp_point in stamp_points) >= min_stamps)

    # Jeden Tag maximal radius Kilometer zurücklegen
    for day in range(days):
        model.addConstr(quicksum(e[day, from_id, to_id]*distance for (from_id, to_id), distance in distances.items()) <= maximum_daily_distance)

    # Nur beschränkt oft den Bus benutzen
    model.addConstr(quicksum(n[day, bus_stop] for day in range(days) for bus_stop in bus_stops) <= max_bus_days)

    model.setObjective(quicksum(e[day, from_id, to_id]*distance for day in range(days) for (from_id, to_id), distance in distances.items()))
    model.optimize()

    if model.Status in [GRB.INFEASIBLE, GRB.INF_OR_UNBD]:
        print("This model is infeasible")
        model.computeIIS()
        model.write("iis.ilp")
        model.write("iis.mps")

    print("Elapsed time:", round(perf_counter()-start_time, 2), "seconds")

    final_distances = {}
    prev = {}
    visited_stamp_points = {}
    for day in range(days):
        final_distances[day] = 0
        prev[day] = {}
        visited_stamp_points[day] = []
        for from_id, to_id in distances:
            if e[day, from_id, to_id].X == 1:
                prev[day][to_id] = from_id
        for stamp_point in stamp_points:
            if n[day, stamp_point].X == 1:
                visited_stamp_points[day].append(stamp_points[stamp_point][2])

    for (day, from_id, to_id), var in e.items():
        if var.X == 1:
            final_distances[day] += distances[from_id, to_id]

    print("")
    print("Gesamtkilometer:", round(sum([distance for day, distance in final_distances.items()])/1000, 2))
    print("")
    for day in range(days):
        print("Besuche", sum([round(n[day, stamp_point].X) for stamp_point in stamp_points]), "stamp_points am Tag", day+1, "mit", round(final_distances[day]/1000, 2), "km.")
        visits = [origin[3]]
        current_node = origin[0]
        while current_node in prev[day]:
            current_node = prev[day].pop(current_node)
            if current_node == origin[0]:
                visits.insert(0, origin[3])
            elif current_node in bus_stops:
                visits.insert(0, bus_stops[current_node][2])
            elif current_node in stamp_points:
                visits.insert(0, stamp_points[current_node][2])
            else:
                print("ERROR")
                print("current node:", current_node)
                exit()
        print(" - via:", ", ".join(visits))
        print(" - visited:", ", ".join(visited_stamp_points[day]))
        print("")
