from typing import Any, TypedDict


class Property(TypedDict):
    property: str
    type: str


class Triplet(TypedDict):
    start: str
    type: str
    end: str


class StructuredSchemaMetadata(TypedDict):
    constraint: list[dict[str, Any]]
    index: list[dict[str, Any]]


class GraphSchema(TypedDict):
    node_props: dict[str, list[Property]]
    rel_props: dict[str, list[Property]]
    relationships: list[Triplet]
    metadata: StructuredSchemaMetadata
