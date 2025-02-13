from neo4j import GraphDatabase
import folium


driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    home = list()
    query = "MATCH (n:start) RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name"
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        home = id, lat, lon, name

    haltestellen = []
    query = "MATCH (n:BusStop) " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name ORDER BY id DESC"
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        haltestellen.append((id, lat, lon, name))

    stempelstellen = []
    query = "MATCH (n:StampPoint) " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name ORDER BY id ASC"
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        name = result.get("name")
        stempelstellen.append((id, lat, lon, name))

    min_lat, min_lon, max_lat, max_lon = None, None, None, None
    for location in haltestellen + stempelstellen:
        if min_lat is None or location[1] < min_lat:
            min_lat = location[1]
        if min_lon is None or location[2] < min_lon:
            min_lon = location[2]
        if max_lat is None or location[1] > max_lat:
            max_lat = location[1]
        if max_lon is None or location[2] > max_lon:
            max_lon = location[2]
    map_center = (min_lat + max_lat) / 2, (min_lon + max_lon) / 2

    # Create a map
    mymap = folium.Map(location=map_center, zoom_start=11, tiles='OpenStreetMap')

    for haltestelle in haltestellen:
        folium.Marker(
            location=haltestelle[1:3],
            popup=folium.Popup(f"<nobr>Haltestelle: {haltestelle[3]}</nobr>"),
            icon=folium.Icon(icon="bus", color="blue", prefix="fa")
        ).add_to(mymap)

    for stempelstelle in stempelstellen:
        folium.Marker(
            location=stempelstelle[1:3],
            popup=folium.Popup(f"<nobr>Haltestelle: {stempelstelle[3]}</nobr>"),
            icon=folium.Icon(icon="stamp", color="green", prefix="fa")
        ).add_to(mymap)

    folium.Marker(
        location=haltestelle[1:3],
        popup=folium.Popup(f"<nobr>Start: {haltestelle[3]}</nobr>"),
        icon=folium.Icon(icon="house", color="red", prefix="fa")
    ).add_to(mymap)

    # Save the map
    mymap.save("map.html")
