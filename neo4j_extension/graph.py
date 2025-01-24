from __future__ import annotations

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


class Entity:
    element_id: str
    properties: PyDict[str, Neo4jType]

    def __init__(
        self,
        element_id: str,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
    ) -> None:
        self.element_id = element_id
        self.properties = {
            k: ensure_neo4j_type(v) for k, v in properties.items()
        }

    def to_python_map(
        self,
        keep_element_id: bool = False,
        element_id_val: Optional[str] = None,
    ) -> dict[str, PythonType]:
        """
        Convert properties to Python basic types(dict).
        If keep_element_id is True, element_id is also stored.
        If element_id_val is not None, it is used as the value of element_id.
        """
        result: dict[str, PythonType] = {}
        for k, v in self.properties.items():
            result[k] = convert_neo4j_to_python(v)
        if keep_element_id and element_id_val is not None:
            result["element_id"] = element_id_val
        return result

    def to_cypher_map(self) -> str:
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


class Node(Entity):
    labels: Set[str]

    def __init__(
        self,
        element_id: str,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        labels: Optional[Set[str]] = None,
    ) -> None:
        super().__init__(element_id, properties)
        self.labels = labels or set()

    def to_cypher_create(self, var_name: str = "n") -> str:
        if self.labels:
            label_str = ":" + ":".join(self.labels)
        else:
            label_str = ""
        props_str = self.to_cypher_map()
        return (
            f"CREATE ({var_name}{label_str} {props_str}) RETURN {var_name}"
        )


class Relationship(Entity):
    rel_type: str
    start_node: Node
    end_node: Node

    def __init__(
        self,
        element_id: str,
        properties: Mapping[str, Union[Neo4jType, PythonType]],
        rel_type: str,
        start_node: Node,
        end_node: Node,
    ) -> None:
        super().__init__(element_id, properties)
        self.rel_type = rel_type
        self.start_node = start_node
        self.end_node = end_node

    def to_cypher_create(
        self,
        var_name: str = "r",
        start_var: str = "a",
        end_var: str = "b",
    ) -> str:
        props_str = self.to_cypher_map()
        escaped_rel_type = _escape_identifier(self.rel_type)
        return (
            f"CREATE ({start_var})-[{var_name}:{escaped_rel_type} {props_str}]->({end_var}) "
            f"RETURN {var_name}"
        )


class Graph:
    def __init__(self):
        self.nodes: PyDict[str, Node] = {}
        self.relationships: PyDict[str, Relationship] = {}

    def add_node(self, node: Node) -> None:
        self.nodes[node.element_id] = node

    def add_relationship(self, relationship: Relationship) -> None:
        self.relationships[relationship.element_id] = relationship

    def remove_node(self, element_id: str) -> None:
        to_remove = []
        for rid, rel in self.relationships.items():
            if (
                rel.start_node.element_id == element_id
                or rel.end_node.element_id == element_id
            ):
                to_remove.append(rid)
        for rid in to_remove:
            self.remove_relationship(rid)
        self.nodes.pop(element_id, None)

    def remove_relationship(self, element_id: str) -> None:
        self.relationships.pop(element_id, None)
