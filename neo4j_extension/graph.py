from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict as PyDict
from typing import List as PyList
from typing import Mapping, Optional, Set, Union

from ._utils import _escape_identifier
from .abc import Neo4jType
from .conversion import (
    PythonType,
    convert_neo4j_to_python,
    ensure_neo4j_type,
)
from .primitives import Neo4jList


class PropertiesDict(PyDict[str, Neo4jType]):
    def __setitem__(self, key: str, value: Neo4jType | PythonType) -> None:
        super().__setitem__(key, ensure_neo4j_type(value))


class Entity(ABC):
    properties: PropertiesDict
    globalId: Optional[str]

    def __init__(
        self,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        globalId: Optional[str] = None,
    ) -> None:
        self.globalId = globalId
        self.properties = PropertiesDict(
            {k: ensure_neo4j_type(v) for k, v in properties.items()}
        )

    def to_python_props(self) -> dict[str, PythonType]:
        """
        Convert properties to Python basic types(dict).
        """
        result: dict[str, PythonType] = {}
        for k, v in self.properties.items():
            result[k] = convert_neo4j_to_python(v)
        if self.globalId:
            result["globalId"] = self.globalId
        return result

    def to_cypher_props(self) -> str:
        pairs: PyList[str] = []
        for k, v in self.properties.items():
            # 리스트인 경우 property 저장 가능 여부 검사
            if isinstance(v, Neo4jList):
                if not v.is_storable_as_property():
                    raise ValueError(
                        f"Property '{k}' contains a non-storable ListValue."
                    )
            escaped_key = _escape_identifier(k)
            pairs.append(f"{escaped_key}: {v.to_cypher()}")
        if not pairs:
            return "{}"
        return "{ " + ", ".join(pairs) + " }"

    @abstractmethod
    def to_cypher(self) -> str: ...

    @property
    def id(self) -> str:
        return f"{_escape_identifier(self.globalId or self.__class__.__name__ + '_' + str(id(self)))}"

    def __repr__(self) -> str:
        return self.to_cypher()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.to_python_props()})"


class Node(Entity):
    labels: Set[str]

    def __init__(
        self,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        labels: Optional[Set[str]] = None,
        globalId: Optional[str] = None,
    ) -> None:
        super().__init__(properties=properties, globalId=globalId)
        self.labels = labels or set()

    def to_cypher(self) -> str:
        return (
            f"({self.id}: {':'.join(self.labels)} {self.to_cypher_props()})"
        )


class Relationship(Entity):
    rel_type: str
    start_node: Node
    end_node: Node

    def __init__(
        self,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        rel_type: str,
        start_node: Node,
        end_node: Node,
        globalId: Optional[str] = None,
    ) -> None:
        super().__init__(properties=properties, globalId=globalId)
        self.rel_type = rel_type
        self.start_node = start_node
        self.end_node = end_node

    def to_cypher(self) -> str:
        props_str = self.to_cypher_props()
        return f"{self.start_node.to_cypher()}-[{self.id}: {self.rel_type} {props_str}]->{self.end_node.to_cypher()}"


class Graph:
    def __init__(self):
        self.nodes: PyDict[str, Node] = {}
        self.relationships: PyDict[str, Relationship] = {}

    def add_node(self, node: Node) -> None:
        self.nodes[node.id] = node

    def add_relationship(self, relationship: Relationship) -> None:
        self.relationships[relationship.id] = relationship

    def remove_node(self, node_id: str) -> None:
        to_remove: PyList[str] = []
        for rid, rel in self.relationships.items():
            if rel.start_node.id == node_id or rel.end_node.id == node_id:
                to_remove.append(rid)
        for rid in to_remove:
            self.remove_relationship(rid)
        self.nodes.pop(node_id, None)

    def remove_relationship(self, rel_id: str) -> None:
        self.relationships.pop(rel_id, None)


if __name__ == "__main__":
    graph = Graph()
    node1 = Node({"name": "Alice"}, {"Person"}, "alice")
    node2 = Node({"name": "Bob"}, {"Person"}, "bob")
    rel = Relationship(
        {"since": 1999}, "KNOWS", node1, node2, "alice_knows_bob"
    )
    graph.add_node(node1)
    graph.add_node(node2)
    graph.add_relationship(rel)
    print(node1)
    print(node2)
    print(rel)
    print(graph.nodes)
    print(graph.relationships)
    graph.remove_node("alice")
    print(graph.nodes)
    print(graph.relationships)
    graph.remove_relationship("alice_knows_bob")
    print(graph.relationships)
