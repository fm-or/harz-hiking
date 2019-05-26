from neo4j.v1 import GraphDatabase
from gurobipy import Model, GRB, quicksum
import time

radius_min = 0
radius_max = 40000
maximum_daily_distance = 45000
days = 3
bus_days = 3
min_stempel = 9

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

    query = "MATCH (n:Haltestelle) WHERE n.name = 'Wohnung' RETURN ID(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name"
    results = session.run(query)
    result = results.__iter__().__next__()
    id = result.get("id")
    lat = result.get("lat")
    lon = result.get("lon")
    name = result.get("name")
    origin_haltestelle = (id, lat, lon, name)

    haltestellen = {}
    query = "WITH point({{longitude: {origin_lon}, latitude: {origin_lat}}}) AS origin " \
            "MATCH (n:Haltestelle) " \
            "WHERE distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) < {radius_max} " \
            "AND (distance(origin, point({{longitude: n.longitude, latitude: n.latitude}})) > {radius_min} OR n.name = 'Wohnung') " \
            "RETURN ID(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name" \
            "".format(origin_lon=origin[2], origin_lat=origin[1], radius_min=radius_min, radius_max=radius_max)
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
            "".format(origin_lon=origin[2], origin_lat=origin[1], radius=radius_max)
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        stempelstellen[id] = (lat, lon, name)

    distances = {}
    edges = {}
    reverse_edges = {}
    query = "MATCH (s)-[r:TO]->(t) " \
            "RETURN ID(s) AS from_id, r.distance AS distance, ID(t) AS to_id"
    for result in session.run(query):
        from_id = result.get("from_id")
        distance = result.get("distance")
        to_id = result.get("to_id")
        distances[from_id, to_id] = distance
        if from_id not in edges.keys():
            edges[from_id] = []
        if to_id not in reverse_edges.keys():
            reverse_edges[to_id] = []
        edges[from_id].append(to_id)
        reverse_edges[to_id].append(from_id)


    def subtourelim(model, where):
        if where == GRB.callback.MIPSOL:
            stempelstellen = model._stempelstellen
            subtour_counter = 0
            prev = {}
            for day in range(days):
                prev[days] = {}
                for from_id in stempelstellen.keys():
                    for to_id in stempelstellen.keys():
                        if from_id != to_id:
                            if model.cbGetSolution(model._vars[day, from_id, to_id]) > 0:
                                prev[days][to_id] = from_id
                while len(prev[days]) > 0:
                    first = next(iter(prev[days].keys()))
                    current = prev[days].pop(first)
                    tour = [current, first]
                    while current in prev[days].keys():
                        current = prev[days].pop(current)
                        tour.insert(0, current)
                    if tour[0] == tour[-1]:
                        subtour_counter += 1
                        model.cbLazy(quicksum(model._vars[day, from_id, to_id] for from_id, to_id in zip(tour[:-1], tour[1:])) <= len(tour)-2)
                        print("\t", tour)
            print(" - Removed", subtour_counter, "subtours")

    print("Untersuche", len(haltestellen), "Haltestellen und", len(stempelstellen), "Stempelstellen")
    model = Model()
    model.setParam("outputFlag", True)
    #model.setParam("MIPGap", 1)
    #model.setParam("MIPGapAbs", days*10000)
    # model.params.LazyConstraints = 1

    n = {}
    x = {}
    for day in range(days):
        x[day, origin[0]] = model.addVar(vtype=GRB.INTEGER)
        for haltestelle in haltestellen.keys():
            n[day, haltestelle] = model.addVar(vtype=GRB.BINARY)
        for haltestelle in haltestellen.keys():
            x[day, haltestelle] = model.addVar(vtype=GRB.CONTINUOUS, lb=0, ub=2*min_stempel)
        for stempelstelle in stempelstellen.keys():
            n[day, stempelstelle] = model.addVar(vtype=GRB.BINARY)
            x[day, stempelstelle] = model.addVar(vtype=GRB.CONTINUOUS, lb=0, ub=2*min_stempel)

    for stempelstelle in stempelstellen.keys():
        n[stempelstelle] = model.addVar(vtype=GRB.BINARY)

    e = {}
    z = {}
    for day in range(days):
        for from_id, to_id in distances.keys():
            e[day, from_id, to_id] = model.addVar(vtype=GRB.BINARY, name='e'+str(day)+'_'+str(from_id)+'_'+str(to_id))
        for stempelstelle in stempelstellen:
            e[day, stempelstelle, stempelstelle] = model.addVar(vtype=GRB.BINARY, lb=0, ub=0)
            for stempelstelle_2 in stempelstellen:
                if stempelstelle_2 != stempelstelle:
                    z[day, stempelstelle, stempelstelle_2] = model.addVar(vtype=GRB.CONTINUOUS)
    model.update()

    '''model.addConstr(n[0, 700] == 1)
    model.addConstr(x[0, 700] == 1)
    model.addConstr(e[0, 700, 168] == 1)
    model.addConstr(n[0, 168] == 1)
    model.addConstr(x[0, 168] == 2)
    model.addConstr(e[0, 168, 0] == 1)'''

    for day in range(days):
        # Jeden Tag genau einen Startknoten benutzen
        model.addConstr(quicksum(n[day, from_id] for from_id in haltestellen.keys()) == 1)

        # Jeden Tag die Startknoten an die Kanten linken
        for from_id in haltestellen.keys():
            model.addConstr(quicksum(e[day, from_id, to_id] for to_id in stempelstellen.keys()) == n[day, from_id])

        # Jeden Tag EXPERIMENT
        '''for haltestelle in haltestellen.keys():
            model.addConstr(z[day, haltestelle] <= n[day, haltestelle]*3*min_stempel)
            model.addConstr(z[day, haltestelle] <= x[day, haltestelle])
            model.addConstr(z[day, haltestelle] >= x[day, haltestelle]-3*min_stempel*(1 - n[day, haltestelle]))'''
        for stempelstelle in stempelstellen.keys():
            for from_id in stempelstellen:
                if from_id != stempelstelle:
                    model.addConstr(z[day, from_id, stempelstelle] <= e[day, from_id, stempelstelle]*3*min_stempel)
                    model.addConstr(z[day, from_id, stempelstelle] <= x[day, from_id] + 1)
                    model.addConstr(z[day, from_id, stempelstelle] >= x[day, from_id] + 1 - 3*min_stempel*(1 - e[day, from_id, stempelstelle]))
            model.addConstr(quicksum(z[day, from_id, stempelstelle] for from_id in stempelstellen if from_id != stempelstelle) <= x[day, stempelstelle])

        # Jeden Tag den Heimatort besuchen
        model.addConstr(quicksum(e[day, from_id, origin[0]] for from_id in stempelstellen) == 1)

        # Jeden Tag durch Stempelstellen weiterlaufen
        for stempelstelle in stempelstellen.keys():
            model.addConstr(quicksum(e[day, from_id, stempelstelle] for from_id in list(haltestellen.keys()) + list(stempelstellen.keys()))
                            == quicksum(e[day, stempelstelle, to_id] for to_id in [origin[0]] + list(stempelstellen.keys())))

        # Jeden Tag den Startknoten speichern
        for from_id in haltestellen.keys():
            model.addConstr(quicksum(e[day, from_id, to_id] for to_id in stempelstellen.keys()) == n[day, from_id])

    # Jeden Tag die besuchten Stempelstellen speichern
    for day in range(days):
        for stempelstelle in stempelstellen.keys():
            model.addConstr(quicksum(e[day, from_id, stempelstelle] for from_id in list(haltestellen.keys()) + list(stempelstellen.keys())) == n[day, stempelstelle])

    # Insgesamt die besuchten Stempelstellen speichern
    for stempelstelle in stempelstellen:
        model.addConstr(quicksum(n[day, stempelstelle] for day in range(days)) == n[stempelstelle])

    # Mindestens 50 Stempelstellen besuchen
    model.addConstr(quicksum(n[stempelstelle] for stempelstelle in stempelstellen.keys()) >= min_stempel)

    # Jeden Tag maximal radius Kilometer zurücklegen
    for day in range(days):
        model.addConstr(quicksum(e[day, from_id, to_id]*distance for (from_id, to_id), distance in distances.items()) <= maximum_daily_distance)

    # Nur beschränkt oft den Bus benutzen
    model.addConstr(quicksum(n[day, origin_haltestelle[0]] for day in range(days)) >= days - bus_days)

    # Entferne einige Subtouren
    eliminate_subtours = False
    if eliminate_subtours:
        start = time.time()
        subtours = 0
        '''query = "MATCH (o:Startpunkt) " \
                "MATCH (o)<-[ro1:TO]-(n1:Stempelstelle)-[r1:TO]->(n2:Stempelstelle)-[r2:TO]->(n1) " \
                "MATCH (o)<-[ro2:TO]-(n2) " \
                "WHERE ro1.distance < {radius} AND ro2.distance < {radius} " \
                "AND r1.distance + r2.distance <= 16000 " \
                "RETURN ID(n1) AS id_1, ID(n2) AS id_2 " \
                "".format(radius=radius_max)
        for result in session.run(query):
            id_1 = result.get("id_1")
            id_2 = result.get("id_2")
            subtours += 1
            for day in range(days):
                model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_1] <= 1)'''
        for id_1 in stempelstellen:
            for id_2 in stempelstellen:
                if id_2 != id_1:
                    subtours += 1
                    for day in range(days):
                        model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_1] <= 1)
        print(subtours, "2-Subtouren verboten in", round(time.time()-start, 2), "Sekunden verboten")

        start = time.time()
        subtours = 0
        query = "MATCH (o:Startpunkt) " \
                "MATCH (o)<-[ro1:TO]-(n1:Stempelstelle)-[r1:TO]->(n2:Stempelstelle)-[r2:TO]->(n3:Stempelstelle)-[r3:TO]->(n1) " \
                "MATCH (o)<-[ro2:TO]-(n2) " \
                "MATCH (o)<-[ro3:TO]-(n3) " \
                "WHERE ro1.distance < {radius} AND ro2.distance < {radius} AND ro3.distance < {radius} " \
                "AND r1.distance + r2.distance + r3.distance <= 32000 " \
                "RETURN ID(n1) AS id_1, ID(n2) AS id_2, ID(n3) AS id_3 " \
                "".format(radius=radius_max)
        for result in session.run(query):
            id_1 = result.get("id_1")
            id_2 = result.get("id_2")
            id_3 = result.get("id_3")
            subtours += 1
            for day in range(days):
                model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_3] + e[day, id_3, id_1] <= 2)
        '''for id_1 in stempelstellen:
            for id_2 in stempelstellen:
                if id_2 not in [id_1]:
                    for id_3 in stempelstellen:
                        if id_3 not in [id_1, id_2]:
                            subtours += 1
                            for day in range(days):
                                model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_3] + e[day, id_3, id_1] <= 2)'''
        print(subtours, "3-Subtouren verboten in", round(time.time()-start, 2), "Sekunden verboten")

        start = time.time()
        subtours = 0
        query = "MATCH (o:Startpunkt) " \
                "MATCH (o)<-[ro1:TO]-(n1:Stempelstelle)-[r1:TO]->(n2:Stempelstelle)-[r2:TO]->(n3:Stempelstelle)-[r3:TO]->(n4:Stempelstelle)-[r4:TO]->(n1) " \
                "MATCH (o)<-[ro2:TO]-(n2) " \
                "MATCH (o)<-[ro3:TO]-(n3) " \
                "MATCH (o)<-[ro4:TO]-(n4) " \
                "WHERE ro1.distance < {radius} AND ro2.distance < {radius} AND ro3.distance < {radius} AND ro4.distance < {radius} " \
                "AND r1.distance + r2.distance + r3.distance + r4.distance <= 40000 " \
                "RETURN ID(n1) AS id_1, ID(n2) AS id_2, ID(n3) AS id_3, ID(n4) AS id_4 " \
                "".format(radius=radius_max)
        for result in session.run(query):
            id_1 = result.get("id_1")
            id_2 = result.get("id_2")
            id_3 = result.get("id_3")
            id_4 = result.get("id_4")
            subtours += 1
            for day in range(days):
                model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_3] + e[day, id_3, id_4] + e[day, id_4, id_1] <= 3)
        '''for id_1 in stempelstellen:
            for id_2 in stempelstellen:
                if id_2 not in [id_1]:
                    for id_3 in stempelstellen:
                        if id_3 not in [id_1, id_2]:
                            for id_4 in stempelstellen:
                                if id_4 not in [id_1, id_2, id_3]:
                                    subtours += 1
                                    for day in range(days):
                                        model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_3] + e[day, id_3, id_4] + e[day, id_4, id_1] <= 3)'''
        print(subtours, "4-Subtouren verboten in", round(time.time()-start, 2), "Sekunden verboten")

        '''subtours = 0
        start = time.time()
        query = "MATCH (o:Startpunkt) " \
                "MATCH (o)<-[ro1:TO]-(n1:Stempelstelle)-[r1:TO]->(n2:Stempelstelle)-[r2:TO]->(n3:Stempelstelle)-[r3:TO]->(n4:Stempelstelle)-[r4:TO]->(n5:Stempelstelle)-[r5:TO]->(n1) " \
                "MATCH (o)<-[ro2:TO]-(n2) " \
                "MATCH (o)<-[ro3:TO]-(n3) " \
                "MATCH (o)<-[ro4:TO]-(n4) " \
                "MATCH (o)<-[ro5:TO]-(n5) " \
                "WHERE ro1.distance < {radius} AND ro2.distance < {radius} AND ro3.distance < {radius} AND ro4.distance < {radius} AND ro5.distance < {radius} " \
                "AND r1.distance + r2.distance + r3.distance + r4.distance + r5.distance < 30000 " \
                "RETURN ID(n1) AS id_1, ID(n2) AS id_2, ID(n3) AS id_3, ID(n4) AS id_4, ID(n5) AS id_5 " \
                "".format(radius=radius_max)
        for result in session.run(query):
            id_1 = result.get("id_1")
            id_2 = result.get("id_2")
            id_3 = result.get("id_3")
            id_4 = result.get("id_4")
            id_5 = result.get("id_5")
            subtours += 1
            for day in range(days):
                model.addConstr(e[day, id_1, id_2] + e[day, id_2, id_3] + e[day, id_3, id_4] + e[day, id_4, id_5] + e[day, id_5, id_1] <= 4)
        print(subtours, "5-Subtouren verboten in", round(time.time()-start, 2), "Sekunden verboten")'''

    f = quicksum(e[day, from_id, to_id]*distances[from_id, to_id] for day in range(days) for (from_id, to_id), distance in distances.items())
    model.addConstr(f >= 29098.9100)
    model._stempelstellen = stempelstellen
    model._vars = e
    model.setObjective(f)
    model.optimize()

    if model.Status in [GRB.INFEASIBLE, GRB.INF_OR_UNBD]:
        print("This model is infeasible")
        model.computeIIS()
        model.write("iis.ilp")
        model.write("iis.mps")

    final_distances = {}
    prev = {}
    visited_stempelstellen = {}
    for day in range(days):
        final_distances[day] = 0
        prev[day] = {}
        visited_stempelstellen[day] = []
        for from_id, to_id in distances.keys():
            if e[day, from_id, to_id].X == 1:
                prev[day][to_id] = from_id
        for stempelstelle in stempelstellen:
            if n[day, stempelstelle].X == 1:
                visited_stempelstellen[day].append(stempelstellen[stempelstelle][2])

    for (day, from_id, to_id), var in e.items():
        if var.X == 1:
            final_distances[day] += distances[from_id, to_id]

    print("Gesamtkilometer:", round(sum([distance for day, distance in final_distances.items()])/1000, 2))
    for day in range(days):
        print("Besuche", sum([n[day, stempelstelle].X for stempelstelle in stempelstellen]), "Stempelstellen am Tag", day+1, "mit", round(final_distances[day]/1000, 2), "km.")
        visits = [origin[3]]
        current_node = origin[0]
        while current_node in prev[day]:
            current_node = prev[day].pop(current_node)
            if current_node in haltestellen.keys():
                visits.insert(0, haltestellen[current_node][2])
            elif current_node in stempelstellen.keys():
                visits.insert(0, stempelstellen[current_node][2])
            else:
                print("ERROR")
                print("current node:", current_node)
                exit()
        print(" - via:", ", ".join(visits))
        print(" - visited:", ", ".join(visited_stempelstellen[day]))
        print(prev[day])
        print("")
