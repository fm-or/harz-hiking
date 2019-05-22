from neo4j.v1 import GraphDatabase, Session
import requests


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    locations = {}
    query = "MATCH (n:Stempelstelle) RETURN n.id AS id, n.latitude AS lat, n.longitude AS lon"
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        locations[id] = (lat, lon)
    print(locations)

    location_values = [[lon, lat] for key, (lat, lon) in locations.items()]

    body = {"locations": location_values[:130],
            "metrics": ["distance"]}  # "duration"

    headers = {
        'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
        'Authorization': '5b3ce3597851110001cf6248d990de15874844ddac1347c5541fbcdd'
    }
    call = requests.post('https://api.openrouteservice.org/v2/matrix/foot-hiking', json=body, headers=headers)

    print(call.status_code, call.reason)
    print(call.text)
