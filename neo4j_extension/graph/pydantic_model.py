from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from typing import (
    Optional,
    Sequence,
    Type,
    TypeVar,
    cast,
)
from uuid import uuid4

import neo4j
from pydantic import BaseModel, Field

from neo4j_extension.utils import generate_new_id

from ..types._utils import PythonType, ensure_python_type
from .structure import Graph, Node, Relationship

E = TypeVar("E", bound="EntityModel")
type PropertyType = int | float | str | bool | None | list[PropertyType]


class PropertyModel(BaseModel):
    """
    Represents a single key-value property for a node or relationship.
    """

    k: str
    v: PropertyType


class EntityModel(BaseModel, ABC):
    """
    Common fields for nodes and relationships.
    """

    properties: list[PropertyModel] = Field(
        default_factory=list,
        description="MUST include ALL key-value properties for this entity from the document.",
    )
    uniqueId: int = Field(description="A unique ID for the entity.")

    @abstractmethod
    def to_neo4j(self, *args, **kwargs) -> neo4j.Entity: ...

    @property
    def python_props(self) -> dict[str, PythonType]:
        return {
            prop.k: ensure_python_type(cast(PythonType, prop.v))
            for prop in self.properties
        }

    @property
    def json_props(self) -> dict[str, PropertyType]:
        return {prop.k: prop.v for prop in self.properties}

    @abstractmethod
    def __add__(self: E, other: E) -> E: ...

    @classmethod
    def merge_properties(cls: Type[E], entities: Sequence[E]) -> E:
        if not entities:
            raise ValueError("No entities to merge.")
        entity: EntityModel = deepcopy(entities[0])
        props: dict[str, PropertyModel] = {
            p.k: p for e in entities for p in e.properties
        }
        entity.properties.clear()
        entity.properties.extend(props.values())
        return entity

    @classmethod
    def merge_with_id(
        cls: Type[E], entities: Sequence[E], uniqueId: int
    ) -> E:
        if not entities:
            raise ValueError("No entities to merge.")
        e = entities[0]
        for entity in entities[1:]:
            e += entity
        e.uniqueId = uniqueId
        return e


class NodeModel(EntityModel):
    """
    A single node in the graph.
    """

    labels: list[str] = Field(
        description="""Labels that categorize this node (e.g., ["Animal"], ["Dog"], ["Animal", "Dog"])."""
    )

    def to_neo4j(self, prefix: str) -> Node:
        return Node(
            properties=self.python_props,
            labels=set(self.labels),
            globalId=f"{prefix}#{self.uniqueId}",
        )

    @property
    def signature(self) -> str:
        labels_key: str = "_".join(sorted(self.labels))
        name_val = str(self.json_props.get("name", ""))
        return f"{labels_key}::{name_val}"

    def __add__(self, other: NodeModel) -> NodeModel:
        merged = NodeModel.merge_properties([self, other])
        merged.labels = list(set(self.labels + other.labels))
        return merged

    def orphan_find_original_node_index(
        self, nodes: list[NodeModel]
    ) -> Optional[int]:
        """
        Given a candidate node, find its original index in 'nodes' by matching uniqueId.
        Return None if not found.
        """
        for i, n in enumerate(nodes):
            if n.uniqueId == self.uniqueId:
                return i
        else:
            return None

    def orphan_find_by_property_similarity(
        self, candidates: list[NodeModel]
    ) -> Optional[NodeModel]:
        if not candidates:
            return None

        PROPERTY_WEIGHTS = {
            "name": 3.0,
            "id": 2.5,
            "identifier": 2.5,
            "title": 2.0,
            "type": 1.5,
        }

        def similarity(a: PropertyType, b: PropertyType) -> float:
            if a is None or b is None:
                return 0.0
            if isinstance(a, str) and isinstance(b, str):
                a_s, b_s = a.lower().strip(), b.lower().strip()
                if a_s == b_s:
                    return 1.0
                max_len = max(len(a_s), len(b_s))
                return 1.0 - (abs(len(a_s) - len(b_s)) / max_len)
            return 1.0 if a == b else 0.0

        orphan_props = self.json_props
        scores = []

        for candidate in candidates:
            candidate_props = candidate.json_props
            total_score = 0.0
            for key, weight in PROPERTY_WEIGHTS.items():
                ov = orphan_props.get(key)
                cv = candidate_props.get(key)
                if ov is not None and cv is not None:
                    total_score += weight * similarity(ov, cv)
            scores.append((total_score, candidate))

        max_score = max((s for s, _ in scores), default=0.0)
        top_candidates = [c for s, c in scores if s == max_score]
        return top_candidates[0] if top_candidates else None

    def orphan_find_by_label_match(
        self, candidates: list[NodeModel]
    ) -> Optional[NodeModel]:
        if not candidates:
            return None
        label_scores = []
        orphan_labels = set(self.labels)

        for candidate in candidates:
            candidate_labels = set(candidate.labels)
            score = len(orphan_labels & candidate_labels)
            label_scores.append((score, candidate))

        if not label_scores:
            return None
        max_score = max(s for s, _ in label_scores)
        top_candidates = [c for s, c in label_scores if s == max_score]
        if len(top_candidates) > 1:
            # tie-break by property similarity
            return self.orphan_find_by_property_similarity(top_candidates)
        return top_candidates[0] if top_candidates else None


class RelationshipModel(EntityModel):
    """
    A single relationship (edge) in the graph.
    """

    type: str = Field(description="The type of this relationship.")
    startNode: NodeModel = Field(
        description="The start node for this relationship."
    )
    endNode: NodeModel = Field(
        description="The end node for this relationship."
    )

    def to_neo4j(
        self, node_map: dict[str, Node], prefix: str
    ) -> Relationship:
        start_neo4j_node = node_map[f"{prefix}#{self.startNode.uniqueId}"]
        end_neo4j_node = node_map[f"{prefix}#{self.endNode.uniqueId}"]
        return Relationship(
            properties=self.python_props,
            rel_type=self.type,
            start_node=start_neo4j_node,
            end_node=end_neo4j_node,
            globalId=f"{prefix}#{self.uniqueId}",
        )

    def __add__(self, other: RelationshipModel) -> RelationshipModel:
        merged = RelationshipModel.merge_properties([self, other])
        merged.type = other.type
        return merged


class GraphModel(BaseModel):
    """
    Contains a collection of nodes and relationships.
    """

    nodes: list[NodeModel] = Field(
        description="List of all nodes in the graph."
    )
    relationships: list[RelationshipModel] = Field(
        description="List of all relationships (edges) in the graph."
    )

    def to_neo4j(self) -> Graph:
        g = Graph()
        node_map: dict[str, Node] = {}

        prefix = uuid4().hex
        for node in self.nodes:
            node.uniqueId = int(node.uniqueId)
        for rel in self.relationships:
            rel.uniqueId = int(rel.uniqueId)

        for node_model in self.nodes:
            node_obj = node_model.to_neo4j(prefix=prefix)
            g.add_node(node_obj)
            node_map[f"{prefix}#{node_model.uniqueId}"] = node_obj

        for rel_model in self.relationships:
            rel_obj = rel_model.to_neo4j(node_map=node_map, prefix=prefix)
            g.add_relationship(rel_obj)

        return g

    @property
    def entities(self) -> list[EntityModel]:
        return list(self.nodes + self.relationships)

    def model_post_init(self, __context: dict) -> None:
        """
        After parsing this model, fix any ID conflicts (merge id duplicates).
        """
        # Detach relationships that refer nodes pointing to non-existent nodes
        node_ids: set[int] = {n.uniqueId for n in self.nodes}
        self.relationships = [
            r
            for r in self.relationships
            if r.startNode.uniqueId in node_ids
            and r.endNode.uniqueId in node_ids
        ]

        id_to_model: defaultdict[int, list[EntityModel]] = defaultdict(list)
        for entity in self.entities:
            id_to_model[entity.uniqueId].append(entity)
        reserved_ids: set[int] = {id for id in id_to_model}

        nodes: list[NodeModel] = []
        relationships: list[RelationshipModel] = []

        for id, elist in id_to_model.items():
            if not elist:
                continue
            nlist: list[NodeModel] = []
            rlist: list[RelationshipModel] = []
            for e in elist:
                if isinstance(e, NodeModel):
                    nlist.append(e)
                elif isinstance(e, RelationshipModel):
                    rlist.append(e)

            if nlist:
                n: Optional[NodeModel] = NodeModel.merge_with_id(
                    entities=nlist, uniqueId=id
                )
            else:
                n = None

            if rlist:
                r: Optional[RelationshipModel] = (
                    RelationshipModel.merge_with_id(
                        entities=rlist, uniqueId=id
                    )
                )
            else:
                r = None

            if n is None:
                if r is None:
                    continue
                else:
                    # Only relationship exists with this ID
                    relationships.append(r)
            else:
                if r is None:
                    # Only node exists with this ID
                    nodes.append(n)
                else:
                    # Both node and relationship exist with the same ID
                    nodes.append(n)
                    new_id: int = generate_new_id(reserved_ids)
                    r.uniqueId = new_id
                    reserved_ids.add(new_id)
                    relationships.append(r)

        self.nodes = nodes
        self.relationships = relationships

    def add_relationships(
        self, rels_to_add: list[RelationshipModel]
    ) -> GraphModel:
        new_relationships = list(self.relationships)
        new_relationships.extend(rels_to_add)
        return GraphModel(nodes=self.nodes, relationships=new_relationships)

    def orphan_find_orphan_node_ids(
        self, components: list[list[int]]
    ) -> list[int]:
        if len(components) <= 1:
            return []
        main_comp: list[int] = max(components, key=len)
        orphans: list[int] = []
        for comp in components:
            if comp is not main_comp:
                for idx in comp:
                    orphans.append(self.nodes[idx].uniqueId)
        return orphans

    def orphan_find_by_graph_topology(
        self, candidates: list[NodeModel]
    ) -> Optional[NodeModel]:
        if not candidates:
            return None

        degree_centrality = defaultdict(int)
        for rel in self.relationships:
            degree_centrality[rel.startNode.uniqueId] += 1
            degree_centrality[rel.endNode.uniqueId] += 1

        scores: list[tuple[int, NodeModel]] = sorted(
            [
                (degree_centrality[candidate.uniqueId], candidate)
                for candidate in candidates
            ],
            key=lambda x: x[0],
        )
        if not scores:
            return None
        highest_score, highest_score_node = scores[-1]
        return highest_score_node

    def orphan_find_central_node(self, nodes: list[NodeModel]) -> NodeModel:
        """
        Return the 'central' node in the subgraph by highest connectivity.
        If none found, fallback to the first node.
        """
        if not nodes:
            raise ValueError("No nodes given to _find_central_node")

        node_ids_in_main: set[int] = {n.uniqueId for n in nodes}
        connection_counts = defaultdict(int)
        for rel in self.relationships:
            if rel.startNode.uniqueId in node_ids_in_main:
                connection_counts[rel.startNode.uniqueId] += 1
            if rel.endNode.uniqueId in node_ids_in_main:
                connection_counts[rel.endNode.uniqueId] += 1

        if not connection_counts:
            return nodes[0]

        max_node_id = max(
            connection_counts, key=lambda k: connection_counts[k]
        )
        return next(n for n in nodes if n.uniqueId == max_node_id)

    def orphan_validate_relationships(
        self, rels_to_add: list[RelationshipModel]
    ) -> bool:
        existing = set(
            (r.startNode.uniqueId, r.endNode.uniqueId, r.type)
            for r in self.relationships
        )
        for r in rels_to_add:
            triple: tuple[int, int, str] = (
                r.startNode.uniqueId,
                r.endNode.uniqueId,
                r.type,
            )
            if triple in existing:
                return False
        return True

    def orphan_infer_relationship_type(
        self, source: NodeModel, target: NodeModel
    ) -> str:
        type_counter: defaultdict[str, int] = defaultdict(int)
        for rel in self.relationships:
            type_counter[rel.type] += 1

        if type_counter:
            # Reuse the most frequent existing relationship type
            common_type: str = max(
                type_counter, key=lambda k: type_counter[k]
            )
            return common_type

        source_labels = "_".join(sorted(source.labels))
        target_labels = "_".join(sorted(target.labels))
        return f"{source_labels}_TO_{target_labels}"

    def orphan_find_heuristic_connection(
        self,
        orphan_data: list[tuple[NodeModel, list[NodeModel]]],
        start_id: int,
        fallback_node: Optional[NodeModel],
    ) -> list[RelationshipModel]:
        new_rels = []
        current_id = start_id

        for orphan, candidates in orphan_data:
            # 1) label-based match
            best_match = orphan.orphan_find_by_label_match(candidates)
            # 2) property similarity
            if not best_match:
                best_match = orphan.orphan_find_by_property_similarity(
                    candidates
                )
            # 3) fallback to topological approach
            if not best_match:
                best_match = self.orphan_find_by_graph_topology(candidates)

            # 4) if still None, fallback to the single "central" node
            target_node = best_match or fallback_node
            if not target_node:
                continue

            rel_type = self.orphan_infer_relationship_type(
                orphan, target_node
            )
            new_rels.append(
                RelationshipModel(
                    uniqueId=current_id,
                    type=rel_type,
                    startNode=orphan,
                    endNode=target_node,
                    properties=[],
                )
            )
            current_id += 1

        return new_rels

    def orphan_build_adjacency(
        self,
    ) -> tuple[list[list[int]], dict[int, int]]:
        node_idx_map: dict[int, int] = {}
        for i, n in enumerate(self.nodes):
            node_idx_map[n.uniqueId] = i

        adjacency = [[] for _ in range(len(self.nodes))]
        for r in self.relationships:
            s_i = node_idx_map[r.startNode.uniqueId]
            e_i = node_idx_map[r.endNode.uniqueId]
            adjacency[s_i].append(e_i)
            adjacency[e_i].append(s_i)
        return adjacency, node_idx_map

    def merge_duplicate_nodes(self) -> GraphModel:
        """
        Merges nodes that share the same 'signature' (labels, name property, etc.).
        Updates relationships to refer to the merged node.
        """
        original_nodes: list[NodeModel] = self.nodes
        relationships: list[RelationshipModel] = self.relationships

        # Group by signature
        signatures: dict[str, list[int]] = {}
        for idx, node in enumerate(original_nodes):
            sig = node.signature
            signatures.setdefault(sig, []).append(idx)

        merge_map: dict[int, int] = {}
        new_nodes: list[NodeModel] = []

        for sig, indices in signatures.items():
            if len(indices) == 1:
                i = indices[0]
                merge_map[i] = len(new_nodes)
                new_nodes.append(original_nodes[i])
            else:
                # Merge multiple nodes
                base_node = original_nodes[indices[0]]
                for i in indices[1:]:
                    base_node = base_node + original_nodes[i]
                merged_idx = len(new_nodes)
                for i in indices:
                    merge_map[i] = merged_idx
                new_nodes.append(base_node)

        # Update relationships to refer to merged nodes
        new_relationships: list[RelationshipModel] = []
        for rel in relationships:
            # Start node
            s_idx: Optional[int] = (
                rel.startNode.orphan_find_original_node_index(original_nodes)
            )
            if s_idx is None or s_idx not in merge_map:
                continue
            new_s: NodeModel = new_nodes[merge_map[s_idx]]

            # End node
            e_idx: Optional[int] = (
                rel.endNode.orphan_find_original_node_index(original_nodes)
            )
            if e_idx is None or e_idx not in merge_map:
                continue
            new_e: NodeModel = new_nodes[merge_map[e_idx]]

            if new_s.uniqueId == new_e.uniqueId:
                # Skip self-loops created by merging
                continue
            new_relationships.append(
                RelationshipModel(
                    uniqueId=rel.uniqueId,
                    type=rel.type,
                    properties=rel.properties,
                    startNode=new_s,
                    endNode=new_e,
                )
            )

        return GraphModel(nodes=new_nodes, relationships=new_relationships)


class OrphanConnectionProposal(BaseModel):
    """Contains proposed relationships for connecting orphan nodes."""

    relationships: list[RelationshipModel] = Field(
        description="Proposed relationships to connect orphan nodes."
    )

    def process_llm_response(self, next_id: int) -> list[RelationshipModel]:
        new_rels = []
        for rel in self.relationships:
            new_rel = RelationshipModel(
                uniqueId=next_id,
                type=rel.type,
                startNode=rel.startNode,
                endNode=rel.endNode,
                properties=rel.properties.copy(),
            )
            new_rels.append(new_rel)
            next_id += 1
        return new_rels


class OrphanNodesFoundException(Exception):
    """
    Raised when orphan nodes are detected and automatically proposed relationships
    fail validation.
    """

    def __init__(
        self,
        message: str,
        partial_graph: GraphModel,
        orphan_node_ids: list[int],
        proposed_relationships: list[RelationshipModel],
    ):
        super().__init__(message)
        self.partial_graph = partial_graph
        self.orphan_node_ids = orphan_node_ids
        self.proposed_relationships = proposed_relationships
