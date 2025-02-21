from dataclasses import dataclass
from typing import ClassVar
from model.node import Node


@dataclass(frozen=True)
class Home(Node):

    fa_icon: ClassVar[str] = 'house'
    icon_color: ClassVar[str] = 'red'

    def __str__(self):
        return "Home"
