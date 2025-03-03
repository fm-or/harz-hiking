from dataclasses import dataclass
from typing import ClassVar
from model.node import Node


@dataclass(frozen=True)
class ParkingLot(Node):

    fa_icon: ClassVar[str] = 'p'
    icon_color: ClassVar[str] = 'blue'

    def __str__(self):
        return "Parking Lot"
