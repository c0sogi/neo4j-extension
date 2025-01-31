# Code v3

from __future__ import annotations

import json
import re
from typing import LiteralString, Optional, cast

from ._abc import Neo4jType

###############################################################################
# POINT (point({ x: ..., y: ..., [z: ...], crs: '...' }))
###############################################################################

POINT_REGEX = re.compile(
    r"""^\s*point\(\s*(\{.*\})\s*\)\s*$""", re.IGNORECASE
)


class PointValue:
    """
    Represents a POINT value in Neo4j, with CRS, X, Y, and optionally Z coordinates.
    """

    __slots__ = ("crs", "x", "y", "z")

    def __init__(
        self,
        crs: LiteralString,
        x: float,
        y: float,
        z: Optional[float] = None,
    ):
        self.crs: LiteralString = crs
        self.x: float = x
        self.y: float = y
        self.z: float | None = z

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PointValue):
            return False
        return (
            self.crs == other.crs
            and self.x == other.x
            and self.y == other.y
            and self.z == other.z
        )

    def __repr__(self) -> str:
        return f"PointValue(crs={self.crs!r}, x={self.x}, y={self.y}, z={self.z})"


class Neo4jPoint(Neo4jType[PointValue]):
    """
    Corresponds to Neo4j's POINT type.
    """

    def __init__(self, value: PointValue):
        self.value = value

    def to_cypher(self) -> LiteralString:
        parts: list[LiteralString] = [
            cast(LiteralString, f"x: {self.value.x}"),
            cast(LiteralString, f"y: {self.value.y}"),
        ]
        if self.value.z is not None:
            parts.append(cast(LiteralString, f"z: {self.value.z}"))
        parts.append(f"crs: '{self.value.crs}'")
        inner = "{ " + ", ".join(parts) + " }"
        return f"point({inner})"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jPoint:
        m = POINT_REGEX.match(cypher_str)
        if not m:
            raise ValueError(f"Invalid Neo4j point literal: {cypher_str}")
        map_str = m.group(1).strip()

        tmp = re.sub(r"'", '"', map_str)
        tmp = re.sub(r"(\w+)\s*:", r'"\1":', tmp)

        try:
            data = json.loads(tmp)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid point map: {map_str}") from e

        if "x" not in data or "y" not in data:
            raise ValueError(
                f"Invalid point map, must contain x,y keys: {cypher_str}"
            )
        crs = data.get("crs", "cartesian")
        x = float(data["x"])
        y = float(data["y"])
        z = data["z"] if "z" in data else None
        if z is not None:
            z = float(z)
        return cls(PointValue(crs=crs, x=x, y=y, z=z))
