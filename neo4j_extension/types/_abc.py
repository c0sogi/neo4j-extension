from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Generic,
    LiteralString,
    TypeVar,
)

T = TypeVar("T")


class Neo4jType(ABC, Generic[T]):
    """
    Base class for Python representations of specific types
    that can be stored in or returned from Neo4j via Neo4j.
    """

    value: T

    @abstractmethod
    def to_cypher(self) -> LiteralString:
        """
        Serialize this Python object into a valid Neo4j literal (or function call).
        """

    @classmethod
    @abstractmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jType:
        """
        Parse/deserialize a Neo4j literal (or function call) into this Python object.
        """

    def __repr__(self) -> LiteralString:
        return self.to_cypher()

    def __str__(self) -> str:
        return str(self.value)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, self.__class__) and self.value == getattr(
            other, "value", None
        )
