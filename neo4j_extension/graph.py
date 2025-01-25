from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, LiteralString, Mapping, Optional, Self, Union
from typing import Dict as PyDict
from typing import List as PyList

import neo4j
import neo4j.graph

from .abc import Neo4jType
from .conversion import (
    PythonType,
    convert_neo4j_to_python,
    ensure_neo4j_type,
    ensure_python_type,
)
from .primitive import Neo4jList
from .utils import escape_identifier


class Entity(ABC):
    properties: PyDict[LiteralString, Neo4jType]
    globalId: Optional[LiteralString]

    def __init__(
        self,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        globalId: Optional[str] = None,
    ) -> None:
        self.globalId = escape_identifier(globalId) if globalId else None
        self.properties = {
            escape_identifier(k): ensure_neo4j_type(v)
            for k, v in properties.items()
        }

    def __getitem__(self, key: str) -> PythonType:
        return ensure_python_type(self.properties[escape_identifier(key)])

    def __setitem__(
        self, key: str, value: Union[Neo4jType, PythonType]
    ) -> None:
        self.properties[escape_identifier(key)] = ensure_neo4j_type(value)

    def to_python_props(self) -> dict[LiteralString, PythonType]:
        """
        Convert properties to Python basic types(dict).
        """
        result: dict[LiteralString, PythonType] = {}
        for k, v in self.properties.items():
            result[k] = convert_neo4j_to_python(v)
        if self.globalId:
            result["globalId"] = self.globalId
        return result

    def to_cypher_props(self) -> LiteralString:
        pairs: PyList[LiteralString] = []
        for k, v in self.properties.items():
            # 리스트인 경우 property 저장 가능 여부 검사
            if isinstance(v, Neo4jList):
                if not v.is_storable_as_property():
                    raise ValueError(
                        f"Property '{k}' contains a non-storable ListValue."
                    )
            escaped_key: LiteralString = escape_identifier(k)
            pairs.append(f"{escaped_key}: {v.to_cypher()}")
        if not pairs:
            return "{}"
        return "{ " + ", ".join(pairs) + " }"

    @abstractmethod
    def to_cypher(self) -> LiteralString: ...

    @classmethod
    @abstractmethod
    def from_neo4j(cls, entity: neo4j.graph.Entity) -> Self: ...

    @property
    def id(self) -> LiteralString:
        return f"{escape_identifier(self.globalId or self.__class__.__name__ + '_' + str(id(self)))}"

    def __repr__(self) -> LiteralString:
        return self.to_cypher()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.to_python_props()})"


class Node(Entity):
    labels: frozenset[LiteralString]

    def __init__(
        self,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        labels: Optional[set[str] | frozenset[str]] = None,
        globalId: Optional[str] = None,
    ) -> None:
        super().__init__(properties=properties, globalId=globalId)
        self.labels = frozenset(
            escape_identifier(label) for label in labels or ()
        )

    @classmethod
    def from_neo4j(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls,
        entity: neo4j.graph.Node,
    ) -> Self:
        properties: PyDict[str, Any] = entity._properties
        globalId = properties.get("globalId")
        if globalId:
            globalId = str(globalId)
        else:
            globalId = None
        return cls(
            properties=properties, labels=entity.labels, globalId=globalId
        )

    def to_cypher(self) -> LiteralString:
        props: LiteralString = self.to_cypher_props()
        return f"({self.id}: {self.label_str} {props})"

    @property
    def label_str(self) -> LiteralString:
        return ":".join(self.labels) if self.labels else "Node"


class Relationship(Entity):
    rel_type: LiteralString
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
        self.rel_type = escape_identifier(rel_type)
        self.start_node = start_node
        self.end_node = end_node

    @classmethod
    def from_neo4j(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls,
        entity: neo4j.graph.Relationship,
    ) -> Self:
        if entity.start_node is None or entity.end_node is None:
            raise ValueError(
                "Relationship must have both a start and end node."
            )
        properties: PyDict[str, Any] = entity._properties
        globalId = properties.get("globalId")
        if globalId:
            globalId = str(globalId)
        else:
            globalId = None
        return cls(
            properties=properties,
            rel_type=entity.type,
            start_node=Node.from_neo4j(entity.start_node),
            end_node=Node.from_neo4j(entity.end_node),
            globalId=globalId,
        )

    def to_cypher(self) -> LiteralString:
        start_node: LiteralString = self.start_node.to_cypher()
        props_str: LiteralString = self.to_cypher_props()
        return f"{start_node}-[{self.id}: {self.rel_type} {props_str}]->{self.end_node.to_cypher()}"


class NodePath:
    relationships: PyList[Relationship]
    start_node: Optional[Node]
    end_node: Optional[Node]

    def __init__(
        self,
        relationships: PyList[Relationship],
        start_node: Optional[Node] = None,
        end_node: Optional[Node] = None,
    ) -> None:
        if start_node is None and end_node is None:
            raise ValueError(
                "NodePath must contain at least one start or end node."
            )
        self.start_node = start_node
        self.end_node = end_node
        if not relationships:
            raise ValueError(
                "NodePath must contain at least one relationship."
            )
        self.relationships = relationships


class Graph:
    def __init__(self):
        self.nodes: PyDict[LiteralString, Node] = {}
        self.relationships: PyDict[LiteralString, Relationship] = {}

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
        self.nodes.pop(escape_identifier(node_id), None)

    def remove_relationship(self, rel_id: str) -> None:
        self.relationships.pop(escape_identifier(rel_id), None)


if __name__ == "__main__":
    print(Node(properties={}).to_cypher())
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
