from dataclasses import dataclass


@dataclass(frozen=True)
class Node:

    neo4j_id: int
    latitude: float
    longitude: float
