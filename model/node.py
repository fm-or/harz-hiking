from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class Node:

    neo4j_id: str
    latitude: float
    longitude: float
    osm_id: int
    fa_icon: ClassVar[str] = 'circle-dot'
    icon_color: ClassVar[str] = 'black'

    def __str__(self) -> str:
        raise NotImplementedError
