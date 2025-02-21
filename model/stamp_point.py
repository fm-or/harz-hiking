from dataclasses import dataclass
from typing import ClassVar
from model.node import Node


@dataclass(frozen=True)
class StampPoint(Node):

    stamp_id: int
    name: str
    fa_icon: ClassVar[str] = 'stamp'
    icon_color: ClassVar[str] = 'green'

    def __str__(self):
        return f"{self.name} ({self.stamp_id})"
