from model.node import Node
from typing import List
import folium
from folium.plugins import TagFilterButton


class Solution:

    def __init__(self, tours: List[List[Node]]) -> None:
        self.tours = tours

    def visualize_html(self, filename: str) -> None:
        # Find the center of the map
        min_lat, min_lon, max_lat, max_lon = None, None, None, None
        for tour in self.tours:
            for node in tour:
                if min_lat is None or node.latitude < min_lat:
                    min_lat = node.latitude
                if min_lon is None or node.longitude < min_lon:
                    min_lon = node.longitude
                if max_lat is None or node.latitude > max_lat:
                    max_lat = node.latitude
                if max_lon is None or node.longitude > max_lon:
                    max_lon = node.longitude
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2

        # Create the map
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12, tiles='OpenStreetMap')

        # Add markers for each node
        for day, tour in enumerate(self.tours):
            for node in tour:
                folium.Marker([node.latitude, node.longitude], tags=[f"day {day+1}"], popup=str(node), icon=folium.Icon(prefix='fa', icon=node.fa_icon, color=node.icon_color)).add_to(m)

        # Add lines for each tour
        for day, tour in enumerate(self.tours):
            folium.PolyLine([(node.latitude, node.longitude) for node in tour], tags=[f"day {day}"], color='darkred').add_to(m) # darkred, blue, darkblue

        # Add a tag filter button for each group
        TagFilterButton([f"day {day+1}" for day in range(len(self.tours))]).add_to(m)

        # Save the map
        m.save(filename)

    def __str__(self) -> str:
        max_stamps = max(len(tour)-2 for tour in self.tours)
        max_days = len(self.tours)
        output_str = ''
        for day, tour in enumerate(self.tours):
            output_str += ("{stamps:>" + str(len(str(max_stamps))) + "} stamps on day {day:>" + str(len(str(max_days))) + "}: ").format(stamps=len(tour)-2,day=day+1)
            output_str += ' -> '.join(str(node) for node in tour)
            output_str += '\n'
        return output_str
