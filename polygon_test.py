import numpy as np
from scipy.spatial import ConvexHull
from shapely.geometry import Polygon, Point
from neo4j import GraphDatabase
import osmnx as ox



driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "12345678"))
with driver.session() as session:
    coordinates = []
    query = "MATCH (n:StampPoint) " \
            "RETURN n.latitude AS lat, n.longitude AS lon"
    for result in session.run(query):
        lat = result.get("lat")
        lon = result.get("lon")
        coordinates.append((lon, lat))

    points = np.array(coordinates)
    
    hull = ConvexHull(coordinates)

    hull_points = points[hull.vertices]
    convex_hull_polygon = Polygon(hull_points)

    to_delete = []
    query = "MATCH (n:BusStop) " \
            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon, n.name AS name ORDER BY id ASC"
    for result in session.run(query):
        id = result.get("id")
        lat = result.get("lat")
        lon = result.get("lon")
        if not convex_hull_polygon.contains(Point(lon, lat)):
            to_delete.append(id)

    print('to_delete:', len(to_delete))

    for id in to_delete:
        query = "MATCH (n:BusStop) " \
                "WHERE elementId(n) = '{id}' " \
                "DETACH DELETE n" \
                "".format(id=id)
        session.run(query)