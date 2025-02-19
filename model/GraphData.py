from typing import Optional
from neo4j import GraphDatabase
import gpxpy
import numpy as np
from scipy.spatial import ConvexHull
from shapely import Polygon, affinity
import overpy
import geopy.distance
import osmnx as ox
import os.path # os.path.isfile
import os # os.remove
import networkx as nx
from time import perf_counter


class GraphData:
    _map_filename = 'cache/graph.graphml'
    _map_enlarge_factor = 1.1
    _threads = 1

    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    def import_data(self, stampPoint_gpx_filename: str, ignore_radius: float = 500.0, max_distance_m: Optional[float] = None, output: bool = False, force_update: bool = False) -> None:
        if force_update:
            self._empty_database()
            self._delete_map()
        else:
            self._ensure_map_database_consistency()

        ox.settings.log_console = output
        if self._map_exists():
            self._load_map()
            if output:
                print("Loaded the map.")
        else:
            new_stampPoint_count = self._import_stampPoints(stampPoint_gpx_filename)
            if new_stampPoint_count > 0 and output:
                print(f"Imported {new_stampPoint_count} stamp points.")

            self._create_map()
            if output:
                print("Created the map.")

            new_busStop_count = self.import_busStops(ignore_radius)
            if new_busStop_count > 0 and output:
                print(f"Imported {new_busStop_count} bus stops.")

            new_parkingLot_count = self.import_parkingLots(ignore_radius)
            if new_parkingLot_count > 0 and output:
                print(f"Imported {new_parkingLot_count} parking lots.")

            self._simplify_map()
            self._save_map()
            if output:
                print("Saved the map.")
        
        if output:
            print("Importing missing distances.")
        self._import_missing_distances(max_distance_m=max_distance_m)
    
    def _empty_database(self) -> None:
        # delete all nodes and relationships
        with self.driver.session() as session:
            query = "MATCH (n) DETACH DELETE n"
            session.run(query)

    def _map_exists(self) -> bool:
        # check if map file exists
        return os.path.isfile(self._map_filename)

    def _delete_map(self) -> None:
        if self._map_exists():
            # delete the map file
            os.remove(self._map_filename)
    
    def _ensure_map_database_consistency(self) -> None:
        if not self._map_exists():
            # empty the database as the input process may have been interrupted
            self._empty_database()

    def _import_stampPoints(self, stampPoint_gpx_filename: str) -> int:
        with self.driver.session() as session, open(stampPoint_gpx_filename, 'r') as gpx_file:
            gpx = gpxpy.parse(gpx_file)
            for stampPoint in gpx.waypoints:
                id = int(stampPoint.name[3:6])
                name = stampPoint.name[7:]
                query = f"CREATE (n:StampPoint {{stamp_id: {id}, name: '{name}', latitude: {stampPoint.latitude}, longitude: {stampPoint.longitude}}})"
                session.run(query)
            return len(gpx.waypoints)
        
    def _get_enclosing_lon_lat_polygon(self) -> Polygon:
        if not hasattr(self, '_enclosing_lon_lat_polygon'):
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
                self._enclosing_lon_lat_polygon = affinity.scale(polygon, xfact=self._map_enlarge_factor, yfact=self._map_enlarge_factor)
        return self._enclosing_lon_lat_polygon
        
    def _create_map(self) -> None:
        with self.driver.session() as session:
            # create an unsimplified map enclosing the stamp points
            self._map = ox.graph_from_polygon(self._get_enclosing_lon_lat_polygon(), network_type='walk', simplify=False, retain_all=False)

            # find the osmid of the nearest node for each stamp point
            query = "MATCH (n:StampPoint) " \
                    "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon"
            neo4j_ids = list()
            lats = list()
            lons = list()
            for result in session.run(query):
                neo4j_ids.append(result.get("id"))
                lats.append(result.get("lat"))
                lons.append(result.get("lon"))
            # find the osmid of the nearest node for each stamp point
            osm_ids = ox.distance.nearest_nodes(self._map, lats, lons)
            for i in range(len(neo4j_ids)):
                # store the osmid
                query = f"MATCH (n) WHERE elementId(n) = '{neo4j_ids[i]}' " \
                        f"SET n.osmid = {osm_ids[i]}"
                session.run(query)
                # mark the node to keep
                self._map.nodes[osm_ids[i]]['keep'] = True
        
    def _import_osm_entities(self, osm_filter: str, osm_output: str, neo4j_tag: str, ignore_radius: float) -> int:
        with self.driver.session() as session:
            # get the polygon enclosing the stamp points
            polygon = self._get_enclosing_lon_lat_polygon()
            polygon_string = ' '.join(f"{lat} {lon}" for lon, lat in polygon.exterior.coords)

            # query the OSM API for the entities
            api = overpy.Overpass()
            results = api.query(f"{osm_filter}" \
                                f"  (poly:'{polygon_string}');" \
                                f"{osm_output}")
            entities = list()
            for node in results.nodes:
                if node.id in self._map.nodes:
                    entities.append((node.id, node.lat, node.lon))
            
            # thin the entities according to ignore_radius
            adjacent_entities = [list() for _ in range(len(entities))]
            for i in range(len(entities)-1):
                for j in range(i+1, len(entities)):
                    #distance_lb = 0.95*ox.distance.great_circle(entities[i][1], entities[i][2], entities[j][1], entities[j][2])
                    distance_lb = 0.95*geopy.distance.great_circle(entities[i][1:3], entities[j][1:3]).m
                    if distance_lb < ignore_radius:
                        distance = geopy.distance.geodesic(entities[i][1:3], entities[j][1:3]).m
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
            
            # insert the thinned entities into the database and mark them to keep
            for id in thinned_ids:
                osmid = entities[id][0]
                lat = entities[id][1]
                lon = entities[id][2]
                query = f"MERGE (n:{neo4j_tag} {{osmid: {osmid}}}) " \
                    f"ON CREATE SET n.latitude = {lat}, n.longitude = {lon}"
                session.run(query)
                self._map.nodes[osmid]['keep'] = True
            return len(thinned_ids)
        
    def import_busStops(self, ignore_radius: float) -> int:
        #return self._import_osm_entities('node[highway=bus_stop][public_transport=platform]', 'out skel;', 'BusStop', ignore_radius)
        return self._import_osm_entities('way[public_transport]', 'node(w);out skel;', 'BusStop', ignore_radius)
    
    def import_parkingLots(self, ignore_radius: float) -> int:
        return self._import_osm_entities('way[amenity=parking][access=yes][fee=no][parking=surface]', 'node(w);out skel;', 'ParkingLot', ignore_radius)

    def _simplify_map(self) -> None:
        self._map = ox.simplify_graph(self._map, node_attrs_include=['keep'], edge_attr_aggs={'length': sum})

    def _save_map(self) -> None:
        ox.save_graphml(self._map, self._map_filename)

    def _load_map(self) -> None:
        self._map = ox.load_graphml(self._map_filename)
    
    def _import_missing_distances(self, max_distance_m: Optional[float]) -> int:
        with self.driver.session() as session:
            # calculate distances of arcs between stamp points if distance is missing and needed (reverse arcs are also updated)
            query = "MATCH (s1:StampPoint)-[r:TO]->(s2:StampPoint) " \
                    "WHERE r.distance IS NULL AND s1.stamp_id < s2.stamp_id " + ("" if max_distance_m is None else f"AND r.lowerBound < {max_distance_m} ") + "" \
                    "RETURN s1.osmid AS osmid1, s2.osmid AS osmid2, elementId(r) AS rid"
            results = session.run(query)
            origin_ids = list()
            destiation_ids = list()
            relation_ids = list()
            for result in results:
                origin_ids.append(result.get("osmid1"))
                destiation_ids.append(result.get("osmid2"))
                relation_ids.append(results.get("rid"))
            if len(origin_ids) > 0:
                routes = ox.routing.shortest_path(self._map, orig=origin_ids, dest=destiation_ids, weight='length', cpus=self._threads)
                for i in range(len(relation_ids)):
                    if len(routes[i]) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self._map, routes[i], weight='length')
                        distance = gdf['length'].sum()
                    query = "MATCH (s1:StampPoint)-[r1:TO]->(s2:StampPoint) " \
                            f"WHERE elementId(r1) = '{relation_ids[i]}' " \
                            "MATCH (s2:StampPoint)-[r2:TO]->(s1:StampPoint) " \
                            f"SET r1 = {{distance: {distance}}}, r2 = {{distance: {distance}}}"
                    session.run(query)

            # calculate distances of arcs between stamp points if arc is missing (reverse arcs are also created)
            query = "MATCH (s1:StampPoint) " \
                    "MATCH (s2:StampPoint) " \
                    "WHERE NOT (s1)-[:TO]->(s2) AND s1.stamp_id < s2.stamp_id " \
                    "RETURN elementId(s1) AS id1, s1.osmid AS osmid1, s1.latitude AS lat1, s1.longitude AS lon1, elementId(s2) AS id2, s2.osmid AS osmid2, s2.latitude AS lat2, s2.longitude AS lon2"
            results = session.run(query)
            origins = list()
            origin_ids = list()
            destinations = list()
            destination_ids = list()
            for result in results:
                distance_lowerBound = 0.95*ox.distance.great_circle(result.get("lat1"), result.get("lon1"), result.get("lat2"), result.get("lon2"))
                if max_distance_m is None or distance_lowerBound < max_distance_m:
                    origins.append(result.get("osmid1"))
                    origin_ids.append(result.get("id1"))
                    destinations.append(result.get("osmid2"))
                    destination_ids.append(result.get("id2"))
                else:
                    query = f"MATCH (s1:StampPoint) WHERE elementId(s1) = '{result.get('id1')}' " \
                            f"MATCH (s2:StampPoint) WHERE elementId(s2) = '{result.get('id2')}' " \
                            f"MERGE (s1)-[r1:TO]->(s2) " \
                            f"ON CREATE SET r1 = {{lowerBound: {distance_lowerBound}}} " \
                            f"MERGE (s2)-[r2:TO]->(s1)" \
                            f"ON CREATE SET r2 = {{lowerBound: {distance_lowerBound}}} "
                    session.run(query)
            if len(origins) > 0:
                routes = ox.routing.shortest_path(self._map, orig=origins, dest=destinations, weight='length', cpus=self._threads)
                for i in range(len(origin_ids)):
                    if len(routes[i]) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self._map, routes[i], weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (s1:StampPoint) WHERE elementId(s1) = '{origin_ids[i]}' " \
                            f"MATCH (s2:StampPoint) WHERE elementId(s2) = '{destination_ids[i]}' " \
                            f"MERGE (s1)-[r1:TO]->(s2) " \
                            f"ON CREATE SET r1 = {{distance: {distance}}} " \
                            f"MERGE (s2)-[r2:TO]->(s1)" \
                            f"ON CREATE SET r2 = {{distance: {distance}}} "
                    session.run(query)

            # calculate distances of arcs not between stamp points if distance is missing and needed
            query = "MATCH (n1)-[r:TO]->(n2) " \
                    "WHERE NOT (n1:StampPoint AND n2:StampPoint) AND r.distance IS NULL " + ("" if max_distance_m is None else f"AND r.lowerBound < {max_distance_m} ") + "" \
                    "RETURN n1.osmid AS osmid1, n2.osmid AS osmid2, elementId(r) AS rid"
            results = session.run(query)
            origin_ids = list()
            destiation_ids = list()
            relation_ids = list()
            for result in results:
                origin_ids.append(result.get("osmid1"))
                destiation_ids.append(result.get("osmid2"))
                relation_ids.append(results.get("rid"))
            if len(origin_ids) > 0:
                routes = ox.routing.shortest_path(self._map, orig=origin_ids, dest=destiation_ids, weight='length', cpus=self._threads)
                for i in range(len(relation_ids)):
                    if len(routes[i]) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self._map, routes[i], weight='length')
                        distance = gdf['length'].sum()
                    query = "MATCH ()-[r:TO]->() " \
                            f"WHERE elementId(r) = '{relation_ids[i]}' " \
                            f"SET r = {{distance: {distance}}}"
                    session.run(query)

            # calculate distances from bus stops to stamp points if arc is missing
            query = "MATCH (b:BusStop) " \
                    "MATCH (s:StampPoint) " \
                    "WHERE NOT (b)-[:TO]->(s) " \
                    "RETURN elementId(b) AS bid, b.osmid AS bosmid, b.latitude AS blat, b.longitude AS blon, elementId(s) AS sid, s.osmid AS sosmid, s.latitude AS slat, s.longitude AS slon"
            results = session.run(query)
            origins = list()
            origin_ids = list()
            destinations = list()
            destination_ids = list()
            for result in results:
                distance_lowerBound = 0.95*ox.distance.great_circle(result.get("blat"), result.get("blon"), result.get("slat"), result.get("slon"))
                if max_distance_m is None or distance_lowerBound < max_distance_m:
                    origins.append(result.get("bosmid"))
                    origin_ids.append(result.get("bid"))
                    destinations.append(result.get("sosmid"))
                    destination_ids.append(result.get("sid"))
                else:
                    query = f"MATCH (b:BusStop) WHERE elementId(b) = '{result.get('bid')}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{result.get('sid')}' " \
                            f"MERGE (b)-[r:TO]->(s) " \
                            f"ON CREATE SET r = {{lowerBound: {distance_lowerBound}}} "
                    session.run(query)
            if len(origins) > 0:
                routes = ox.routing.shortest_path(self._map, orig=origins, dest=destinations, weight='length', cpus=self._threads)
                for i in range(len(origin_ids)):
                    if len(routes[i]) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self._map, routes[i], weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (b:BusStop) WHERE elementId(b) = '{origin_ids[i]}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{destination_ids[i]}' " \
                            f"CREATE (b)-[:TO {{distance: {distance}}}]->(s)"
                    session.run(query)

            # calculate distances from parking lots to stamp points if arc is missing
            query = "MATCH (p:ParkingLot) " \
                    "MATCH (s:StampPoint) " \
                    "WHERE NOT (p)-[:TO]->(s) " \
                    "RETURN elementId(p) AS pid, p.osmid AS posmid, p.latitude AS plat, p.longitude AS plon, elementId(s) AS sid, s.osmid AS sosmid, s.latitude AS slat, s.longitude AS slon"
            results = session.run(query)
            origins = list()
            origin_ids = list()
            destinations = list()
            destination_ids = list()
            for result in results:
                distance_lowerBound = 0.95*ox.distance.great_circle(result.get("plat"), result.get("plon"), result.get("slat"), result.get("slon"))
                if max_distance_m is None or distance_lowerBound < max_distance_m:
                    origins.append(result.get("posmid"))
                    origin_ids.append(result.get("pid"))
                    destinations.append(result.get("sosmid"))
                    destination_ids.append(result.get("sid"))
                else:
                    query = f"MATCH (p:ParkingLot) WHERE elementId(p) = '{result.get('pid')}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{result.get('sid')}' " \
                            f"MERGE (p)-[r1:TO]->(s) " \
                            f"ON CREATE SET r1 = {{lowerBound: {distance_lowerBound}}} " \
                            f"MERGE (s)-[r2:TO]->(p) " \
                            f"ON CREATE SET r2 = {{lowerBound: {distance_lowerBound}}} "
                    session.run(query)
            if len(origins) > 0:
                routes = ox.routing.shortest_path(self._map, orig=origins, dest=destinations, weight='length', cpus=self._threads)
                for i in range(len(origin_ids)):
                    if len(routes[i]) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self._map, routes[i], weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (p:ParkingLot) WHERE elementId(p) = '{origin_ids[i]}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{destination_ids[i]}' " \
                            f"CREATE (p)-[:TO {{distance: {distance}}}]->(s) " \
                            f"CREATE (s)-[:TO {{distance: {distance}}}]->(p) "
                    session.run(query)