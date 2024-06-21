from dataclasses import dataclass
from pathlib import Path
from enum import Enum

class ResourceType(Enum):
    SEED = "seed"
    SOURCE = "source"
    MODEL = "model"
    SNAPSHOT = "snapshot"

@dataclass
class Column:
    name: str
    data_type: str
    description: str


@dataclass
class Resource:
    name: str
    type: ResourceType
    path: Path
    columns: list[Column]
