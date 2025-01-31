from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
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

from ..types._utils import PythonType, ensure_python_type
from .structure import Graph, Node, Relationship

E = TypeVar("E", bound="EntityModel")
type PropertyType = int | float | str | bool | None | list[PropertyType]


class PropertyModel(BaseModel):
    """
    Represents a single key-value property for a node or relationship.
    """

    key: str
    value: PropertyType


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
            prop.key: ensure_python_type(cast(PythonType, prop.value))
            for prop in self.properties
        }

    @property
    def json_props(self) -> dict[str, PropertyType]:
        return {prop.key: prop.value for prop in self.properties}

    @classmethod
    def merge_properties(cls: Type[E], entities: Sequence[E]) -> E:
        """
        For property normalization, if multiple entities are merged, combine
        their properties and unify duplicates in a list-like manner.
        """
        if not entities:
            raise ValueError("No entities to merge.")
        entity: EntityModel = entities[0]
        properties: list[PropertyModel] = []
        for e in entities:
            properties.extend(e.properties)

        normalized_props: dict[str, list[PropertyType] | PropertyType] = {}
        for p in properties:
            # Example: unify string properties by lowercasing
            normalized_value: PropertyType = (
                p.value.strip().lower()
                if isinstance(p.value, str)
                else p.value
            )
            if p.key in normalized_props:
                existing_val: PropertyType = normalized_props[p.key]
                if not isinstance(existing_val, list):
                    existing_val = [existing_val]
                existing_val.append(normalized_value)
                normalized_props[p.key] = existing_val
            else:
                normalized_props[p.key] = normalized_value

        # If there's only a single value in the list, flatten it
        for k, v in normalized_props.items():
            if isinstance(v, list) and len(v) == 1:
                normalized_props[k] = v[0]

        entity.properties.clear()
        entity.properties.extend(
            [
                PropertyModel(key=k, value=v)
                for k, v in normalized_props.items()
            ]
        )

        if isinstance(entity, NodeModel):
            merged_labels: list[str] = []
            for e in (e for e in entities if isinstance(e, NodeModel)):
                merged_labels.extend(e.labels)
            merged_labels = sorted(set(merged_labels))
            entity.labels = merged_labels

        return entity


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
        """
        Merges two nodes: unify labels, unify property values, handle duplicates.
        """
        merged_labels = sorted(set(self.labels + other.labels))
        a_props = self.json_props
        b_props = other.json_props

        merged_props = {}
        all_keys = set(a_props.keys()).union(b_props.keys())

        for key in all_keys:
            a_val = a_props.get(key)
            b_val = b_props.get(key)

            if a_val is None:
                merged_props[key] = b_val
            elif b_val is None:
                merged_props[key] = a_val
            else:
                # Merge them into a list, removing duplicates
                if isinstance(a_val, list) and isinstance(b_val, list):
                    merged_list = a_val + b_val
                elif isinstance(a_val, list):
                    merged_list = a_val + [b_val]
                elif isinstance(b_val, list):
                    merged_list = [a_val] + b_val
                else:
                    merged_list = [a_val, b_val]

                # Deduplicate
                seen = set()
                deduped = []
                for item in merged_list:
                    item_str = str(item)
                    if item_str not in seen:
                        seen.add(item_str)
                        deduped.append(item)
                # If there's only one unique item, flatten
                merged_props[key] = (
                    deduped if len(deduped) > 1 else deduped[0]
                )

        merged_props_list = [
            PropertyModel(key=key, value=value)
            for key, value in merged_props.items()
        ]
        return NodeModel(
            uniqueId=self.uniqueId,
            labels=merged_labels,
            properties=merged_props_list,
        )

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
        start_neo4j_node = node_map[f"#{self.startNode.uniqueId}"]
        end_neo4j_node = node_map[f"#{self.endNode.uniqueId}"]
        return Relationship(
            properties=self.python_props,
            rel_type=self.type,
            start_node=start_neo4j_node,
            end_node=end_neo4j_node,
            globalId=f"{prefix}#{self.uniqueId}",
        )


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
        After parsing this model, fix any ID conflicts (merge duplicates).
        """
        self.resolve_merge_conflicts()

    def resolve_merge_conflicts(self, id_start_from: int = 1) -> None:
        """
        개선된 resolve_merge_conflicts 함수.

        1) 노드끼리, 관계끼리 각각 ID 충돌을 해소하여 병합(중복 uniqueId가 같은 엔티티들을 합침).
        - NODE ↔ RELATIONSHIP 간 ID 충돌이 있을 경우, 관계의 ID를 자동 재할당하여 회피.

        2) 관계 병합 시 startNode, endNode, type)가 완전히 같은 관계들을 하나로 합쳐서 중복 에지를 제거한다(멀티에지 불허).

        3) 최종적으로 노드와 관계 각각에 대해 ID를 재할당하여, 정렬된 순서로 1부터 다시 부여.
        (id_start_from 파라미터로 시작값 조정 가능)

        4) 관계에서 참조하는 startNode, endNode의 ID도 재할당 ID로 매핑해주고,
        유효하지 않은 참조(삭제된 노드)는 제거한다.
        """

        # --- 0) 기존 리스트를 복제해서 작업 ---
        original_nodes = list(self.nodes)
        original_rels = list(self.relationships)

        # ------------------------------------------------------------------
        # 1) NODE들의 uniqueId 충돌 처리
        # ------------------------------------------------------------------
        # uniqueId별로 묶어서, 같은 ID를 가진 노드들끼리는 합침(NODE.merge_properties 이용)
        node_id_map: defaultdict[int, list[NodeModel]] = defaultdict(list)
        for node in original_nodes:
            node_id_map[node.uniqueId].append(node)

        merged_nodes: list[NodeModel] = []
        for old_id, same_id_nodes in node_id_map.items():
            if len(same_id_nodes) == 1:
                # 충돌 없음
                merged_nodes.append(same_id_nodes[0])
            else:
                # 같은 uniqueId 가진 노드들끼리 병합
                merged_node = NodeModel.merge_properties(same_id_nodes)
                merged_nodes.append(merged_node)

        # 이제 merged_nodes에 중복 병합이 끝난 노드들이 들어 있음
        # (아직 uniqueId가 겹쳐 있을 수 있음)

        # ------------------------------------------------------------------
        # 2) 관계 ID와 노드 ID가 충돌하는 경우 해소
        # ------------------------------------------------------------------
        #   - Node/Relationship가 같은 ID를 쓸 수 없으므로,
        #     Node와 충돌하는 Relationship ID를 자동 재할당
        node_id_set: set[int] = {n.uniqueId for n in merged_nodes}
        rel_id_set: set[int] = {r.uniqueId for r in original_rels}

        # 교집합 찾기
        conflict_ids: set[int] = node_id_set.intersection(rel_id_set)
        if conflict_ids:
            # 충돌하는 관계 ID를 새로 배정해 준다
            next_temp_id: int = max(node_id_set.union(rel_id_set)) + 1
            for rel in original_rels:
                if rel.uniqueId in conflict_ids:
                    rel.uniqueId = next_temp_id
                    next_temp_id += 1

        # ------------------------------------------------------------------
        # 3) RELATIONSHIP들의 uniqueId 충돌 처리
        # ------------------------------------------------------------------
        # uniqueId별로 묶어서, 같은 ID를 가진 관계들은 병합 가능 여부 체크
        # (만약 type이 서로 다르면 병합 불가능 → ID 재할당 or 예외처리)
        rel_id_map: defaultdict[int, list[RelationshipModel]] = defaultdict(
            list
        )
        for rel in original_rels:
            rel_id_map[rel.uniqueId].append(rel)

        merged_rels: list[RelationshipModel] = []
        for old_id, same_id_rels in rel_id_map.items():
            if len(same_id_rels) == 1:
                # 충돌 없음
                merged_rels.append(same_id_rels[0])
            else:
                # 같은 uniqueId를 가진 관계 여러 개
                # 1) type이 전부 같은지 확인
                all_types: set[str] = {r.type for r in same_id_rels}
                if len(all_types) == 1:
                    # 동일 type이면 RELATIONSHIP.merge_properties 활용
                    merged_rel = RelationshipModel.merge_properties(
                        same_id_rels
                    )
                    merged_rels.append(merged_rel)
                else:
                    # type이 다르다면 병합 불가능.
                    # 여기서는 "자동으로 새 ID를 할당"해 각각 살리는 예시를 보이지만,
                    # 상황에 따라 예외를 던지는 정책도 가능.
                    for r in same_id_rels:
                        r.uniqueId = id_start_from
                        id_start_from += 1
                        merged_rels.append(r)

        # ------------------------------------------------------------------
        # 4) 노드 ID, 관계 ID 재할당 (최종적으로 오름차순, or 기존 순 정렬)
        # ------------------------------------------------------------------
        new_nodes_sorted = sorted(merged_nodes, key=lambda n: n.uniqueId)
        new_rels_sorted = sorted(merged_rels, key=lambda r: r.uniqueId)

        # 노드부터 순서대로 id_start_from 부여
        new_id_map_for_nodes: dict[int, int] = {}
        current_id: int = id_start_from

        for node in new_nodes_sorted:
            old_node_id = node.uniqueId
            new_id_map_for_nodes[old_node_id] = current_id
            node.uniqueId = current_id
            current_id += 1

        # 관계도 이어서 ID 부여
        new_id_map_for_rels = {}
        for rel in new_rels_sorted:
            old_rel_id = rel.uniqueId
            new_id_map_for_rels[old_rel_id] = current_id
            rel.uniqueId = current_id
            current_id += 1

        # ------------------------------------------------------------------
        # 5) 관계에서 참조하는 노드 ID를 새로 매핑 + 유효성 체크
        # ------------------------------------------------------------------
        valid_rels: list[RelationshipModel] = []
        for rel in new_rels_sorted:
            s_old = rel.startNode.uniqueId
            e_old = rel.endNode.uniqueId
            # startNode, endNode도 병합 후의 ID로 교체
            if (
                s_old not in new_id_map_for_nodes
                or e_old not in new_id_map_for_nodes
            ):
                # 유효하지 않은 노드 참조 → 제거하거나 에러 처리
                continue
            rel.startNode.uniqueId = new_id_map_for_nodes[s_old]
            rel.endNode.uniqueId = new_id_map_for_nodes[e_old]
            valid_rels.append(rel)

        # ------------------------------------------------------------------
        # 6) (start, end, type)가 완전히 같은 관계를 다시 병합할지 결정
        # ------------------------------------------------------------------

        rel_key_map = defaultdict(list)
        for r in valid_rels:
            key = (r.startNode.uniqueId, r.endNode.uniqueId, r.type)
            rel_key_map[key].append(r)

        final_rels = []
        for same_edges in rel_key_map.values():
            if len(same_edges) == 1:
                final_rels.append(same_edges[0])
            else:
                # 같은 (start, end, type)에 대해 RELATIONSHIP.merge_properties
                merged_edge = RelationshipModel.merge_properties(same_edges)
                final_rels.append(merged_edge)

        # ------------------------------------------------------------------
        # 최종 결과를 self에 반영
        # ------------------------------------------------------------------
        self.nodes.clear()
        self.nodes.extend(new_nodes_sorted)

        self.relationships.clear()
        self.relationships.extend(final_rels)

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
