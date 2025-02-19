from dataclasses import dataclass
from model.node import Node


@dataclass(frozen=True)
class StampPoint(Node):

    stamp_id: int
    name: str
