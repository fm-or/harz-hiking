from model.node import Node
from model.stamp_point import StampPoint
from model.bus_stop import BusStop
from model.parking_lot import ParkingLot
from typing import Dict, Tuple

class Solution:

    def __init__(self, tours: Dict[int, Tuple[Node]]) -> None:
        self.tours = tours

    def visualize_html(self, filename: str) -> None:
        pass

    def __str__(self) -> str:
        pass
