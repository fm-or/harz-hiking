"""
GraphData class for managing and importing geographical data into a Neo4j database.
"""

import json
import os  # os.remove
import os.path  # os.path.isfile
from typing import Dict, Optional
from neo4j import GraphDatabase
import gpxpy
import numpy as np
import scipy.spatial
from shapely import Polygon, affinity
import overpy
import geopy.distance
import matplotlib.pyplot as plt
import networkx as nx
import osmnx as ox
from model.node import Node
from model.stamp_point import StampPoint
from model.bus_stop import BusStop
from model.parking_lot import ParkingLot
from model.home import Home


class GraphData:
    """
    A class to manage and process graph data for hiking routes using Neo4j and OSM data.
    """

    _map_filename = 'cache/graph.graphml'
    _map_enlarge_factor = 1.1
    _threads = 1
    _home_filename = 'cache/home.json'

    def __init__(self,
                 neo4j_uri: str,
                 neo4j_user: str,
                 neo4j_password: str,
                 neo4j_database: Optional[str] = None):
        self._driver = GraphDatabase.driver(
            neo4j_uri, auth=(neo4j_user, neo4j_password))
        self._neo4j_database = neo4j_database
        self._map = None
        self._enclosing_lon_lat_polygon = None
        self._stamp_points = None
        self._bus_stops = None
        self._parking_lots = None
        self._distances = None
        self._distances_reverse = None

    @property
    def stamp_points(self) -> Dict[int, StampPoint]:
        """
        Returns a dictionary of stamp points as a dictionary of tuples.
        """
        if self._stamp_points is None:
            with self._neo4j_session() as session:
                query = "MATCH (n:StampPoint) " \
                        "WHERE EXISTS { " \
                        "    MATCH (n)-[r:TO]-() " \
                        "    WHERE r.distance IS NOT NULL " \
                        "} " \
                        "RETURN elementId(n) AS neo4j_id, n.latitude AS latitude, n.longitude AS longitude, n.osmid AS osmid, n.stamp_id AS stamp_id, n.name AS name"
                self._stamp_points = {result.get("neo4j_id"): StampPoint(result.get("neo4j_id"),
                                                                         result.get("latitude"),
                                                                         result.get("longitude"),
                                                                         result.get("osmid"),
                                                                         result.get("stamp_id"),
                                                                         result.get("name"))
                                      for result in session.run(query)}
        return self._stamp_points

    @property
    def bus_stops(self) -> Dict[int, BusStop]:
        """
        Returns a dictionary of bus stops as a dictionary of tuples.
        """
        if self._bus_stops is None:
            with self._neo4j_session() as session:
                query = "MATCH (n:BusStop) " \
                        "WHERE EXISTS { " \
                        "    MATCH (n)-[r:TO]-() " \
                        "    WHERE r.distance IS NOT NULL " \
                        "} " \
                        "RETURN elementId(n) AS neo4j_id, n.latitude AS latitude, n.longitude AS longitude, n.osmid AS osmid"
                self._bus_stops = {result.get("neo4j_id"): BusStop(result.get("neo4j_id"),
                                                                   result.get("latitude"),
                                                                   result.get("longitude"),
                                                                   result.get("osmid"))
                                   for result in session.run(query)}
        return self._bus_stops

    @property
    def parking_lots(self) -> Dict[int, ParkingLot]:
        """
        Returns a dictionary of parking lots as a dictionary of tuples.
        """
        if self._parking_lots is None:
            with self._neo4j_session() as session:
                query = "MATCH (n:ParkingLot) " \
                        "WHERE EXISTS { " \
                        "    MATCH (n)-[r:TO]-() " \
                        "    WHERE r.distance IS NOT NULL " \
                        "} " \
                        "RETURN elementId(n) AS neo4j_id, n.latitude AS latitude, n.longitude AS longitude, n.osmid AS osmid"
                self._parking_lots = {result.get("neo4j_id"): ParkingLot(result.get("neo4j_id"),
                                                                         result.get("latitude"),
                                                                         result.get("longitude"),
                                                                         result.get("osmid"))
                                      for result in session.run(query)}
        return self._parking_lots
    
    def node(self, neo4j_id: int) -> Node:
        """
        Returns the node with the given neo4j ID.
        """
        if neo4j_id in self.stamp_points:
            return self.stamp_points[neo4j_id]
        elif neo4j_id in self.bus_stops:
            return self.bus_stops[neo4j_id]
        elif neo4j_id in self.parking_lots:
            return self.parking_lots[neo4j_id]
        else:
            raise ValueError(f"Node with neo4j ID {neo4j_id} not found.")

    @property
    def distances(self) -> Dict[int, Dict[int, float]]:
        """
        Returns a dictionary of distances between nodes as a dictionary of dictionaries.
        """
        if self._distances is None:
            with self._neo4j_session() as session:
                query = "MATCH (s)-[r:TO]->(t) " \
                        "WHERE r.distance IS NOT NULL " \
                        "RETURN elementId(s) AS from_id, elementId(t) AS to_id, r.distance AS distance"
                self._distances = dict()
                self._distances_reverse = dict()
                for result in session.run(query):
                    from_id = result.get("from_id")
                    to_id = result.get("to_id")
                    distance = result.get("distance")
                    if from_id not in self._distances:
                        self._distances[from_id] = dict()
                    if to_id not in self._distances_reverse:
                        self._distances_reverse[to_id] = dict()
                    self._distances[from_id][to_id] = distance
                    self._distances_reverse[to_id][from_id] = distance
        return self._distances
    
    @property
    def distances_reverse(self) -> Dict[int, Dict[int, float]]:
        """
        Returns a dictionary of distances between nodes as a dictionary of dictionaries.
        """
        self.distances  # ensure that self._distances_reverse is initialized
        return self._distances_reverse

    @property
    def map(self) -> nx.DiGraph:
        if self._map is None:
            self._load_map()
        return self._map
    
    def is_arc(self, from_id: int, to_id: int) -> bool:
        """
        Returns True if there is an arc from the node with the given neo4j ID to the node with the other given neo4j ID.
        """
        return from_id in self.distances and to_id in self.distances[from_id]
    
    def distance(self, from_id: int, to_id: int) -> float:
        """
        Returns the distance of the arc from the node with the given neo4j ID to the node with the other given neo4j ID.
        """
        return self.distances[from_id][to_id]

    def import_data(self,
                    stamp_point_gpx_filename: str,
                    ignore_radius: float = 500.0,
                    max_section_length_m: Optional[float] = None,
                    log: bool = False,
                    force_update: bool = False) -> None:
        """
        Imports data from various sources and updates the map and database accordingly.
        """
        if force_update:
            self._empty_database()
            self._delete_map()
        else:
            self._ensure_map_database_consistency()

        ox.settings.log_console = log
        if not self._map_exists():
            new_stamp_point_count = self._import_stamp_points(
                stamp_point_gpx_filename)
            if new_stamp_point_count > 0 and log:
                print(f"Imported {new_stamp_point_count} stamp points.")

            self._create_map()
            if log:
                print("Created the map.")

            new_bus_stop_count = self._import_bus_stops(ignore_radius)
            if new_bus_stop_count > 0 and log:
                print(f"Imported {new_bus_stop_count} bus stops.")

            new_parking_lot_count = self._import_parking_lots(ignore_radius)
            if new_parking_lot_count > 0 and log:
                print(f"Imported {new_parking_lot_count} parking lots.")

            self._simplify_map()
            self._save_map()
            if log:
                print("Saved the map.")

        if log:
            print("Importing missing distances.")
        self._import_missing_distances(
            max_section_length_m=max_section_length_m)

    def _neo4j_session(self):
        if self._neo4j_database is None:
            return self._driver.session()
        else:
            return self._driver.session(database=self._neo4j_database)

    def _empty_database(self) -> None:
        # delete all nodes and relationships
        with self._neo4j_session() as session:
            # insert some nodes and relationships to avoid unknown labels and properties
            query = "CREATE (:BusStation {latitude: 0.0, longitude: 0.0, osmid: 0})" \
                "-[:TO {distance: 0.0, lowerBound: 0.0}]->" \
                    "(:StampPoint {name: 'dummy', latitude: 0.0, longitude: 0.0, osmid: 0})" \
                "-[:TO {distance: 0.0, lowerBound: 0.0}]->" \
                "(:ParkingLot {latitude: 0.0, longitude: 0.0, osmid: 0})"
            session.run(query)
            # delete all nodes and relationships
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

    def _import_stamp_points(self, stamp_point_gpx_filename: str) -> int:
        with (self._neo4j_session() as session,
              open(stamp_point_gpx_filename, 'r', encoding="utf-8") as gpx_file):
            gpx = gpxpy.parse(gpx_file)
            for stamp_point in gpx.waypoints:
                stamp_id = int(stamp_point.name[3:6])
                name = stamp_point.name[7:]
                query = "CREATE (n:StampPoint {" \
                    f"stamp_id: {stamp_id}, name: '{name}', " \
                    f"latitude: {stamp_point.latitude}, longitude: {stamp_point.longitude}" \
                    "})"
                session.run(query)
            return len(gpx.waypoints)

    def _get_enclosing_lon_lat_polygon(self) -> Polygon:
        if self._enclosing_lon_lat_polygon is None:
            with self._neo4j_session() as session:
                query = "MATCH (n:StampPoint) " \
                        "RETURN n.latitude AS lat, n.longitude AS lon"
                result_list = list(session.run(query))
                points = np.empty([len(result_list), 2], dtype=np.float64)
                for i, result in enumerate(result_list):
                    lon = result.get("lon")
                    lat = result.get("lat")
                    points[i] = (lon, lat)
                hull = scipy.spatial.ConvexHull(points)
                hull_points = points[hull.vertices]
                polygon = Polygon(hull_points)
                self._enclosing_lon_lat_polygon = affinity.scale(polygon,
                                                                 xfact=self._map_enlarge_factor,
                                                                 yfact=self._map_enlarge_factor)
        return self._enclosing_lon_lat_polygon

    def _create_map(self) -> None:
        with self._neo4j_session() as session:
            # create an unsimplified map enclosing the stamp points
            self._map = ox.graph_from_polygon(self._get_enclosing_lon_lat_polygon(),
                                              network_type='walk',
                                              simplify=False,
                                              retain_all=False)

            # find the osmid of the nearest node for each stamp point
            query = "MATCH (n:StampPoint) " \
                    "RETURN elementId(n) AS id, n.latitude AS lat, n.longitude AS lon"
            neo4j_ids = []
            lats = []
            lons = []
            for result in session.run(query):
                neo4j_ids.append(result.get("id"))
                lats.append(result.get("lat"))
                lons.append(result.get("lon"))
            # find the osmid of the nearest node for each stamp point
            osm_ids = ox.distance.nearest_nodes(self._map, lons, lats)
            for i, neo4j_id in enumerate(neo4j_ids):
                # store the osmid
                query = f"MATCH (n) WHERE elementId(n) = '{neo4j_id}' " \
                        f"SET n.osmid = {osm_ids[i]}"
                session.run(query)
                # mark the node to keep
                self._map.nodes[osm_ids[i]]['keep'] = True

    def _import_osm_entities(self,
                             osm_filter: str,
                             osm_output: str,
                             neo4j_tag: str,
                             ignore_radius: float) -> int:
        with self._neo4j_session() as session:
            # get the polygon enclosing the stamp points
            polygon = self._get_enclosing_lon_lat_polygon()
            polygon_string = ' '.join(
                f"{lat} {lon}" for lon, lat in polygon.exterior.coords)

            # query the OSM API for the entities
            api = overpy.Overpass()
            results = api.query(f"{osm_filter}"
                                f"  (poly:'{polygon_string}');"
                                f"{osm_output}")
            entities = []
            for node in results.nodes:
                if node.id in self._map.nodes:
                    entities.append((node.id, node.lat, node.lon))

            # thin the entities according to ignore_radius
            adjacent_entities = [[] for _ in range(len(entities))]
            for i in range(len(entities)-1):
                for j in range(i+1, len(entities)):
                    distance_lb = 0.95*geopy.distance.great_circle(entities[i][1:3],
                                                                   entities[j][1:3]).m
                    if distance_lb < ignore_radius:
                        distance = geopy.distance.geodesic(
                            entities[i][1:3], entities[j][1:3]).m
                        if distance < ignore_radius:
                            adjacent_entities[i].append(j)
                            adjacent_entities[j].append(i)
            indizes_sorted = sorted(list(range(len(entities))),
                                    key=lambda i: len(adjacent_entities[i]), reverse=True)

            thinned_indizes = []
            deleted_indizes = []
            for i in indizes_sorted:
                if i not in deleted_indizes:
                    thinned_indizes.append(i)
                    for j in adjacent_entities[i]:
                        deleted_indizes.append(j)

            # insert the thinned entities into the database and mark them to keep
            for i in thinned_indizes:
                osmid = entities[i][0]
                lat = entities[i][1]
                lon = entities[i][2]
                query = f"MERGE (n:{neo4j_tag} {{osmid: {osmid}}}) " \
                    f"ON CREATE SET n.latitude = {lat}, n.longitude = {lon}"
                session.run(query)
                self._map.nodes[osmid]['keep'] = True
            return len(thinned_indizes)

    def _import_bus_stops(self, ignore_radius: float) -> int:
        return self._import_osm_entities('way[public_transport]',
                                         'node(w);out skel;',
                                         'BusStop',
                                         ignore_radius)

    def _import_parking_lots(self, ignore_radius: float) -> int:
        return self._import_osm_entities(
            'way[amenity=parking][access=yes][fee=no][parking=surface]',
            'node(w);out skel;',
            'ParkingLot',
            ignore_radius)

    def _simplify_map(self) -> None:
        self._map = ox.simplify_graph(self._map, node_attrs_include=[
                                      'keep'], edge_attr_aggs={'length': sum})

    def _save_map(self) -> None:
        ox.save_graphml(self._map, self._map_filename)


    def _load_map(self) -> None:
        self._map = ox.load_graphml(self._map_filename)
        #ox.plot_graph(self._map, node_color='r', node_size=.2, edge_linewidth=0.1, show=False, save=True, filepath='map.png', dpi=1200)

    def _calculate_home_cache(self, address: str) -> None:
        if os.path.isfile(self._home_filename):
            with open(self._home_filename, 'r', encoding='utf-8') as file:
                cache_data = json.load(file)
            if cache_data['address'] == address:
                return
        maximum_search_distance_m = 100
        home_coords = ox.geocoder.geocode(address)
        home_map = ox.graph.graph_from_address(address, dist=maximum_search_distance_m, network_type='walk', simplify=False, retain_all=False)
        try:
            home_osmid = ox.distance.nearest_nodes(home_map, [home_coords[0]], [home_coords[1]])[0]
        except Exception as exc:
            raise RuntimeError(f"Could not find a node within {maximum_search_distance_m} m of the address '{address}'.") from exc
        combine_map = nx.compose(self.map, home_map)
        stamp_neo4j_ids = []
        origin_ids = []
        destination_ids = []
        for stamp_point in self.stamp_points.values():
            stamp_neo4j_ids.append(stamp_point.neo4j_id)
            origin_ids.append(home_osmid)
            destination_ids.append(stamp_point.osm_id)
        routes = ox.routing.shortest_path(combine_map, origin_ids, destination_ids, weight='length', cpus=self._threads)
        distances = {}
        for i, route in enumerate(routes):
            if len(route) == 1:
                distances[stamp_neo4j_ids[i]] = 0
            else:
                gdf = ox.routing.route_to_gdf(combine_map, route, weight='length')
                distances[stamp_neo4j_ids[i]] = gdf['length'].sum()
        cache_data = {'address': address, 'latitude': home_coords[0], 'longitude': home_coords[1], 'osmid': int(home_osmid), 'distances': distances}
        with open(self._home_filename, 'w', encoding='utf-8') as file:
            json.dump(cache_data, file)

    def get_home_node(self, address: str, neo4j_id: str) -> Home:
        self._calculate_home_cache(address)
        with open(self._home_filename, 'r', encoding='utf-8') as file:
            cache_data = json.load(file)
            return Home(neo4j_id, cache_data['latitude'], cache_data['longitude'], cache_data['osmid'])

    def get_home_stamp_distances(self, address: str) -> Dict[int, float]:
        self._calculate_home_cache(address)
        with open(self._home_filename, 'r', encoding='utf-8') as file:
            cache_data = json.load(file)
            return cache_data['distances']
    
    def _import_missing_distances(self, max_section_length_m: Optional[float]) -> int:
        with self._neo4j_session() as session:
            # calculate distances of arcs between stamp points if distance is missing and needed
            query = "MATCH (s1:StampPoint)-[r:TO]->(s2:StampPoint) " \
                    "WHERE r.distance IS NULL AND s1.stamp_id < s2.stamp_id " \
                    "" + ("" if max_section_length_m is None else f"AND r.lowerBound < {max_section_length_m} ") + "" \
                    "RETURN s1.osmid AS osmid1, s2.osmid AS osmid2, elementId(r) AS rid"
            results = session.run(query)
            origin_osmids = []
            destiation_osmids = []
            relation_osmids = []
            for result in results:
                origin_osmids.append(result.get("osmid1"))
                destiation_osmids.append(result.get("osmid2"))
                relation_osmids.append(result.get("rid"))
            if len(origin_osmids) > 0:
                routes = ox.routing.shortest_path(
                    self.map, orig=origin_osmids, dest=destiation_osmids,
                    weight='length', cpus=self._threads)
                for i, route in enumerate(routes):
                    if len(route) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(self.map, route, weight='length')
                        distance = gdf['length'].sum()
                    query = "MATCH (s1:StampPoint)-[r1:TO]->(s2:StampPoint) " \
                            f"WHERE elementId(r1) = '{relation_osmids[i]}' " \
                            "MATCH (s2:StampPoint)-[r2:TO]->(s1:StampPoint) " \
                            f"SET r1 = {{distance: {distance}}}, r2 = {{distance: {distance}}}"
                    session.run(query)

            # calculate distances of arcs between stamp points if arc is missing (reverse arcs are also created)
            query = "MATCH (s1:StampPoint) " \
                    "MATCH (s2:StampPoint) " \
                    "WHERE NOT (s1)-[:TO]->(s2) AND s1.stamp_id < s2.stamp_id " \
                    "RETURN elementId(s1) AS id1, s1.osmid AS osmid1, s1.latitude AS lat1, s1.longitude AS lon1, elementId(s2) AS id2, s2.osmid AS osmid2, s2.latitude AS lat2, s2.longitude AS lon2"
            results = session.run(query)
            origins = []
            origin_osmids = []
            destinations = []
            destination_ids = []
            for result in results:
                distance_lower_bound = 0.95*ox.distance.great_circle(result.get(
                    "lat1"), result.get("lon1"), result.get("lat2"), result.get("lon2"))
                if max_section_length_m is None or distance_lower_bound < max_section_length_m:
                    origins.append(result.get("osmid1"))
                    origin_osmids.append(result.get("id1"))
                    destinations.append(result.get("osmid2"))
                    destination_ids.append(result.get("id2"))
                else:
                    query = f"MATCH (s1:StampPoint) WHERE elementId(s1) = '{result.get('id1')}' " \
                            f"MATCH (s2:StampPoint) WHERE elementId(s2) = '{result.get('id2')}' " \
                            f"MERGE (s1)-[r1:TO]->(s2) " \
                            f"ON CREATE SET r1 = {{lowerBound: {distance_lower_bound}}} " \
                            f"MERGE (s2)-[r2:TO]->(s1)" \
                            f"ON CREATE SET r2 = {{lowerBound: {distance_lower_bound}}} "
                    session.run(query)
            if len(origins) > 0:
                routes = ox.routing.shortest_path(
                    self.map, orig=origins, dest=destinations, weight='length', cpus=self._threads)
                for i, route in enumerate(routes):
                    if len(route) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(
                            self.map, route, weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (s1:StampPoint) WHERE elementId(s1) = '{origin_osmids[i]}' " \
                            f"MATCH (s2:StampPoint) WHERE elementId(s2) = '{destination_ids[i]}' " \
                            f"MERGE (s1)-[r1:TO]->(s2) " \
                            f"ON CREATE SET r1 = {{distance: {distance}}} " \
                            f"MERGE (s2)-[r2:TO]->(s1)" \
                            f"ON CREATE SET r2 = {{distance: {distance}}} "
                    session.run(query)

            # calculate distances of arcs not between stamp points if distance is missing and needed
            query = "MATCH (n1)-[r:TO]->(n2) " \
                    "WHERE NOT (n1:StampPoint AND n2:StampPoint) AND r.distance IS NULL " + ("" if max_section_length_m is None else f"AND r.lowerBound < {max_section_length_m} ") + "" \
                    "RETURN n1.osmid AS osmid1, n2.osmid AS osmid2, elementId(r) AS rid"
            results = session.run(query)
            origin_osmids = []
            destiation_osmids = []
            relation_osmids = []
            for result in results:
                origin_osmids.append(result.get("osmid1"))
                destiation_osmids.append(result.get("osmid2"))
                relation_osmids.append(result.get("rid"))
            if len(origin_osmids) > 0:
                routes = ox.routing.shortest_path(
                    self.map, orig=origin_osmids, dest=destiation_osmids, weight='length', cpus=self._threads)
                for i, route in enumerate(routes):
                    if len(route) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(
                            self.map, route, weight='length')
                        distance = gdf['length'].sum()
                    query = "MATCH ()-[r:TO]->() " \
                            f"WHERE elementId(r) = '{relation_osmids[i]}' " \
                            f"SET r = {{distance: {distance}}}"
                    session.run(query)

            # calculate distances from bus stops to stamp points if arc is missing
            query = "MATCH (b:BusStop) " \
                    "MATCH (s:StampPoint) " \
                    "WHERE NOT (b)-[:TO]->(s) " \
                    "RETURN elementId(b) AS bid, b.osmid AS bosmid, b.latitude AS blat, b.longitude AS blon, elementId(s) AS sid, s.osmid AS sosmid, s.latitude AS slat, s.longitude AS slon"
            results = session.run(query)
            origins = []
            origin_osmids = []
            destinations = []
            destination_ids = []
            for result in results:
                distance_lower_bound = 0.95*ox.distance.great_circle(result.get(
                    "blat"), result.get("blon"), result.get("slat"), result.get("slon"))
                if max_section_length_m is None or distance_lower_bound < max_section_length_m:
                    origins.append(result.get("bosmid"))
                    origin_osmids.append(result.get("bid"))
                    destinations.append(result.get("sosmid"))
                    destination_ids.append(result.get("sid"))
                else:
                    query = f"MATCH (b:BusStop) WHERE elementId(b) = '{result.get('bid')}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{result.get('sid')}' " \
                            f"MERGE (b)-[r:TO]->(s) " \
                            f"ON CREATE SET r = {{lowerBound: {distance_lower_bound}}} "
                    session.run(query)
            if len(origins) > 0:
                routes = ox.routing.shortest_path(
                    self.map, orig=origins, dest=destinations, weight='length', cpus=self._threads)
                for i, route in enumerate(routes):
                    if len(route) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(
                            self.map, route, weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (b:BusStop) WHERE elementId(b) = '{origin_osmids[i]}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{destination_ids[i]}' " \
                            f"CREATE (b)-[:TO {{distance: {distance}}}]->(s)"
                    session.run(query)

            # calculate distances from parking lots to stamp points if arc is missing
            query = "MATCH (p:ParkingLot) " \
                    "MATCH (s:StampPoint) " \
                    "WHERE NOT (p)-[:TO]->(s) " \
                    "RETURN elementId(p) AS pid, p.osmid AS posmid, p.latitude AS plat, p.longitude AS plon, " \
                    "elementId(s) AS sid, s.osmid AS sosmid, s.latitude AS slat, s.longitude AS slon"
            results = session.run(query)
            origins = []
            origin_osmids = []
            destinations = []
            destination_ids = []
            for result in results:
                distance_lower_bound = 0.95*ox.distance.great_circle(result.get(
                    "plat"), result.get("plon"), result.get("slat"), result.get("slon"))
                if max_section_length_m is None or distance_lower_bound < max_section_length_m:
                    origins.append(result.get("posmid"))
                    origin_osmids.append(result.get("pid"))
                    destinations.append(result.get("sosmid"))
                    destination_ids.append(result.get("sid"))
                else:
                    query = f"MATCH (p:ParkingLot) WHERE elementId(p) = '{result.get('pid')}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{result.get('sid')}' " \
                            f"MERGE (p)-[r1:TO]->(s) " \
                            f"ON CREATE SET r1 = {{lowerBound: {distance_lower_bound}}} " \
                            f"MERGE (s)-[r2:TO]->(p) " \
                            f"ON CREATE SET r2 = {{lowerBound: {distance_lower_bound}}} "
                    session.run(query)
            if len(origins) > 0:
                routes = ox.routing.shortest_path(
                    self.map, orig=origins, dest=destinations, weight='length', cpus=self._threads)
                for i, route in enumerate(routes):
                    if len(route) == 1:
                        distance = 0
                    else:
                        gdf = ox.routing.route_to_gdf(
                            self.map, route, weight='length')
                        distance = gdf['length'].sum()
                    query = f"MATCH (p:ParkingLot) WHERE elementId(p) = '{origin_osmids[i]}' " \
                            f"MATCH (s:StampPoint) WHERE elementId(s) = '{destination_ids[i]}' " \
                            f"CREATE (p)-[:TO {{distance: {distance}}}]->(s) " \
                            f"CREATE (s)-[:TO {{distance: {distance}}}]->(p) "
                    session.run(query)
