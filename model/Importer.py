from neo4j import GraphDatabase
import gpxpy
import numpy as np
from scipy.spatial import ConvexHull
from shapely import Polygon, affinity
import overpy
import geopy.distance
import osmnx as ox
from os.path import isfile
import networkx as nx
from time import perf_counter


class Importer:
    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def import_stampPoints(self, stampPoint_gpx_filename: str, force_update: bool = False) -> int:
        with self.driver.session() as session, open(stampPoint_gpx_filename, 'r') as gpx_file:
            query = "MATCH (n:StampPoint) " \
                "RETURN COUNT(n) AS count"
            result = session.run(query)
            current_stampPoint_count = result.single().get("count")

            if current_stampPoint_count > 0 and not force_update:
                return 0

            gpx = gpxpy.parse(gpx_file)
            for stampPoint in gpx.waypoints:
                id = int(stampPoint.name[3:6])
                name = stampPoint.name[7:]
                query = f"MERGE (n:StampPoint {{id: {id}}}) " \
                    f"ON CREATE SET n.name = '{name}', n.latitude = {stampPoint.latitude}, n.longitude = {stampPoint.longitude}"
                session.run(query)
            
            new_stampPoint_count = len(gpx.waypoints) - current_stampPoint_count
            return new_stampPoint_count
        
    def _get_enclosing_lon_lat_polygon(self) -> Polygon:
        with self.driver.session() as session:
            query = "MATCH (n:StampPoint) " \
                    "RETURN n.latitude AS lat, n.longitude AS lon"
            result_list = list(session.run(query))
            points = np.empty([len(result_list), 2], dtype=np.float64)
            for i, result in enumerate(result_list):
                lat = result.get("lon")
                lon = result.get("lat")
                points[i] = (lat, lon)
            hull = ConvexHull(points)
            hull_points = points[hull.vertices]
            polygon = Polygon(hull_points)
            return polygon
        
    def _import_osm_entities(self, osm_filter: str, neo4j_tag: str, ignore_radius: float, force_update: bool = False) -> int:
        with self.driver.session() as session:
            query = f"MATCH (n:{neo4j_tag}) " \
                "RETURN COUNT(n) AS count"
            result = session.run(query)
            current_entities_count = result.single().get("count")

            if current_entities_count > 0 and not force_update:
                return 0

            polygon = self._get_enclosing_lon_lat_polygon()
            enlarged_polygon = affinity.scale(polygon, xfact=1.1, yfact=1.1)
            polygon_string = ' '.join(f"{lat} {lon}" for lon, lat in enlarged_polygon.exterior.coords)

            api = overpy.Overpass()
            results = api.query("node" \
                               f"  {osm_filter}" \
                               f"  (poly:'{polygon_string}');" \
                               "out;")
            entities = list()
            for node in results.nodes:
                entities.append((node.tags.get("name"), node.lat, node.lon))
            adjacent_entities = [list() for _ in range(len(entities))]

            for i in range(len(entities)-1):
                for j in range(i+1, len(entities)):
                    distance_lb = 0.95*geopy.distance.great_circle(entities[i][1:], entities[j][1:]).m
                    if distance_lb < ignore_radius:
                        distance = geopy.distance.geodesic(entities[i][1:], entities[j][1:]).m
                        if distance < ignore_radius:
                            adjacent_entities[i].append(j)
                            adjacent_entities[j].append(i)
            ids_sorted = sorted(list(range(len(entities))), key=lambda i: len(adjacent_entities[i]), reverse=True)

            thinned_ids = list()
            deleted_ids = list()
            for i in ids_sorted:
                if i not in deleted_ids:
                    thinned_ids.append(i)
                    for j in adjacent_entities[i]:
                        deleted_ids.append(j)
            
            for id in thinned_ids:
                name = entities[id][0]
                lat = entities[id][1]
                lon = entities[id][2]
                query = f"MERGE (n:{neo4j_tag} {{osmid: {id}}}) " \
                    f"ON CREATE SET n.latitude = {lat}, n.longitude = {lon}, n.name = '{name}'"
                session.run(query)

            new_entities_count = len(thinned_ids) - current_entities_count
            # todo: update cached graph
            return new_entities_count
        
    def import_busStops(self, ignore_radius: float, force_update: bool = False) -> int:
        return self._import_osm_entities('[highway=bus_stop][public_transport=platform]', 'BusStop', ignore_radius, force_update)
    
    def import_parkingLots(self, ignore_radius: float, force_update: bool = False) -> int:
        return self._import_osm_entities('[amenity=parking]', 'ParkingLot', ignore_radius, force_update)

    def _graph(self) -> ox.graph:
        if not hasattr(self, 'graph'):
            if isfile('cache/graph.graphml'):
                self.graph = ox.load_graphml('cache/graph.graphml')
            else:
                detailed_graph = ox.graph_from_polygon(self._get_enclosing_lon_lat_polygon(), network_type='walk', simplify=False, retain_all=False)
                with self.driver.session() as session:
                    query = "MATCH (n:StampPoint) " \
                            "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon"
                    neo4j_ids = list()
                    keep_nodes_lon = list()
                    keep_nodes_lat = list()
                    for result in session.run(query):
                        neo4j_ids.append(result.get("id"))
                        keep_nodes_lon.append(result.get("lon"))
                        keep_nodes_lat.append(result.get("lat"))
                    keep_osmids = ox.distance.nearest_nodes(detailed_graph, keep_nodes_lon, keep_nodes_lat)

                    for i in range(len(neo4j_ids)):
                        osmid = keep_osmids[i]
                        if osmid not in detailed_graph.nodes:
                            raise ValueError(f"Node {osmid} not in graph")
                        query = "MATCH (n) " \
                            f"WHERE elementId(n) = '{neo4j_ids[i]}'" \
                            f"SET n.osmid = {osmid}"
                        detailed_graph.nodes[osmid]['keep'] = True

                    
                    query = "MATCH (n) " \
                            "WHERE n:BusStop OR n:ParkingLot " \
                            "RETURN n.osmid AS osmid"
                    for result in session.run(query):
                        osmid = result.get("osmid")
                        if osmid not in detailed_graph.nodes:
                            raise ValueError(f"Node {osmid} not in graph")
                        detailed_graph.nodes[osmid]['keep'] = True

                    detailed_graph = ox.simplify_graph(detailed_graph, node_attrs_include=['keep'], edge_attr_aggs={'length': sum})
                self.graph = detailed_graph
                ox.save_graphml(self.graph, 'cache/graph.graphml')
        return self.graph
    
    def import_distances(self, max_distance_m: float) -> int:
        with self.driver.session() as session:
            query = "MATCH (s1:StampPoint)-[r:TO]->(s2:StampPoint) " \
                    "WHERE r.distance IS NULL " \
                    "RETURN s1.osmid AS osmid1, s2.osmid AS osmid2, elementId(r) AS rid, r.lowerBound as lowerBound"
            results = session.run(query)
            origins = list()
            destiations = list()
            relation_ids = list()
            for result in results:
                if result.get("lowerBound") < max_distance_m:
                    origins.append(result.get("osmid1"))
                    destiations.append(result.get("osmid2"))
                    relation_ids.append(results.get("rid"))
            if len(origins) > 0:
                routes = ox.routing.shortest_path(self._graph(), orig=origins, dest=destiations, weight='length', cpus=1)
                for i in range(len(relation_ids)):
                    # todo: update relation with distance
                    pass

            query = "MATCH (s1:StampPoint) " \
                    "MATCH (s2:StampPoint) " \
                    "WHERE s1 <> s2 AND NOT (s1)-[:TO]->(s2) " \
                    "RETURN elementId(s1) AS id1, s1.osmid AS osmid1, elementId(s2) AS id2, s2.osmid AS osmid2"
            results = session.run(query)
            origins = list()
            origin_ids = list()
            destinations = list()
            destination_ids = list()
            for result in results:
                origins.append(result.get("osmid1"))
                origin_ids.append(result.get("id1"))
                destinations.append(result.get("osmid2"))
                destination_ids.append(result.get("id2"))
            if len(origins) > 0:
                print("todo", len(origins))
                routes = ox.routing.shortest_path(self._graph(), orig=origins, dest=destinations, weight='length', cpus=1)
                for i in range(len(origin_ids)):
                    if len(routes[i]) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self._graph(), routes[i], weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (s1:StampPoint) WHERE elementId(s1) = '{origin_ids[i]}' " \
                            f"MATCH (s2:StampPoint) WHERE elementId(s2) = '{destination_ids[i]}' " \
                            f"CREATE (s1)-[:TO {{distance: {distance}, lowerBound: {distance}}}]->(s2)"
                    session.run(query)