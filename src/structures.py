from dataclasses import dataclass


@dataclass
class File:
    name: str
    mdate: int
    size: int
    path: str

    def __eq__(self, other):
        return self.name == other.name
