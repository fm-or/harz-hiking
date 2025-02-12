from neo4j import GraphDatabase
import json


with open('bus_stop.geojson') as f:
    data = json.load(f)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    for feature in data['features']:
        if "name" in feature["properties"]:
            name = feature['properties']['name']
            longitude, latitude = feature['geometry']['coordinates']

            query = "MATCH (home:start) WITH point({{latitude: home.latitude, longitude: home.longitude}}) AS home " \
                    "WHERE point.distance(home, point({{latitude: {latitude}, longitude: {longitude}}})) < 40000 " \
                    "MERGE (n:BusStop {{name: '{name}'}}) ON CREATE SET n.latitude = {latitude}, n.longitude = {longitude}" \
                    "".format(latitude=latitude, longitude=longitude, name=name)
            session.run(query)
