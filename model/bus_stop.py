from dataclasses import dataclass
from typing import ClassVar
from model.node import Node


@dataclass(frozen=True)
class BusStop(Node):

    fa_icon: ClassVar[str] = 'bus-simple'
    icon_color: ClassVar[str] = 'orange'

    def __str__(self):
        return "Bus Stop"
