from typing import Literal, Self, Union

from pydantic import BaseModel, model_validator

from .pydantic_model import (
    GraphModel,
    NodeModel,
    PropertyModel,
    PropertyType,
    RelationshipModel,
)


class AddNodeAction(BaseModel):
    type: Literal["AddNode"]
    nodes: list[NodeModel]

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "AddNode"
        else:
            values.type = "AddNode"
        return values


class RemoveNodeAction(BaseModel):
    type: Literal["RemoveNode"]
    nodeIds: list[int]

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "RemoveNode"
        else:
            values.type = "RemoveNode"
        return values


class AddRelationshipAction(BaseModel):
    type: Literal["AddRelationship"]
    relationships: list[RelationshipModel]

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "AddRelationship"
        else:
            values.type = "AddRelationship"
        return values


class RemoveRelationshipAction(BaseModel):
    type: Literal["RemoveRelationship"]
    relationshipIds: list[int]

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "RemoveRelationship"
        else:
            values.type = "RemoveRelationship"
        return values


class AddPropertyAction(BaseModel):
    type: Literal["AddProperty"]
    entityType: Literal["node", "relationship"]
    entityId: int
    property: PropertyModel

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "AddProperty"
        else:
            values.type = "AddProperty"
        return values

    def apply_add_property(
        self,
        base_nodes: dict[int, NodeModel],
        added_nodes: dict[int, NodeModel],
        base_rels: dict[int, RelationshipModel],
        added_rels: dict[int, RelationshipModel],
        node_id_remap: dict[int, int],
        rel_id_remap: dict[int, int],
        removed_node_ids: set[int],
        removed_rel_ids: set[int],
    ) -> bool:
        """Returns True if successfully applied, or False if skipped."""
        eid = self.entityId
        if self.entityType == "node":
            if eid in node_id_remap:
                eid = node_id_remap[eid]
            if eid in removed_node_ids:
                print(
                    f"WARNING: AddPropertyAction on removed node #{eid}. Skip."
                )
                return False
            target = added_nodes.get(eid) or base_nodes.get(eid)
            if not target:
                print(
                    f"WARNING: AddPropertyAction: node #{eid} not found. Skip."
                )
                return False
            # Check if property already exists
            if any(p.key == self.property.key for p in target.properties):
                print(
                    f"WARNING: property {self.property.key} already exists. Skip."
                )
                return False
            target.properties.append(self.property)
            return True

        elif self.entityType == "relationship":
            if eid in rel_id_remap:
                eid = rel_id_remap[eid]
            if eid in removed_rel_ids:
                print(
                    f"WARNING: AddPropertyAction on removed relationship #{eid}. Skip."
                )
                return False
            target = added_rels.get(eid) or base_rels.get(eid)
            if not target:
                print(
                    f"WARNING: AddPropertyAction: relationship #{eid} not found. Skip."
                )
                return False
            if any(p.key == self.property.key for p in target.properties):
                print(
                    f"WARNING: property {self.property.key} already exists. Skip."
                )
                return False
            target.properties.append(self.property)
            return True

        else:
            print(f"WARNING: Invalid entityType {self.entityType}. Skip.")
            return False


class UpdatePropertyAction(BaseModel):
    type: Literal["UpdateProperty"]
    entityType: Literal["node", "relationship"]
    entityId: int
    propertyKey: str
    newValue: PropertyType

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "UpdateProperty"
        else:
            values.type = "UpdateProperty"
        return values

    def apply_update_property(
        self,
        base_nodes: dict[int, NodeModel],
        added_nodes: dict[int, NodeModel],
        base_rels: dict[int, RelationshipModel],
        added_rels: dict[int, RelationshipModel],
        node_id_remap: dict[int, int],
        rel_id_remap: dict[int, int],
        removed_node_ids: set[int],
        removed_rel_ids: set[int],
    ) -> bool:
        eid = self.entityId
        if self.entityType == "node":
            if eid in node_id_remap:
                eid = node_id_remap[eid]
            if eid in removed_node_ids:
                print(
                    f"WARNING: UpdatePropertyAction on removed node #{eid}. Skip."
                )
                return False
            target = added_nodes.get(eid) or base_nodes.get(eid)
            if not target:
                print(f"WARNING: node #{eid} not found for update. Skip.")
                return False
            for prop in target.properties:
                if prop.key == self.propertyKey:
                    prop.value = self.newValue
                    return True
            print(
                f"WARNING: node #{eid} has no property {self.propertyKey}. Skip."
            )
            return False

        elif self.entityType == "relationship":
            if eid in rel_id_remap:
                eid = rel_id_remap[eid]
            if eid in removed_rel_ids:
                print(
                    f"WARNING: UpdatePropertyAction on removed relationship #{eid}. Skip."
                )
                return False
            target = added_rels.get(eid) or base_rels.get(eid)
            if not target:
                print(
                    f"WARNING: relationship #{eid} not found for update. Skip."
                )
                return False
            for prop in target.properties:
                if prop.key == self.propertyKey:
                    prop.value = self.newValue
                    return True
            print(
                f"WARNING: relationship #{eid} has no property {self.propertyKey}. Skip."
            )
            return False

        else:
            print(f"WARNING: invalid entityType {self.entityType}. Skip.")
            return False


class RemovePropertyAction(BaseModel):
    type: Literal["RemoveProperty"]
    entityType: Literal["node", "relationship"]
    entityId: int
    propertyKey: str

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "RemoveProperty"
        else:
            values.type = "RemoveProperty"
        return values

    def apply_remove_property(
        self,
        base_nodes: dict[int, NodeModel],
        added_nodes: dict[int, NodeModel],
        base_rels: dict[int, RelationshipModel],
        added_rels: dict[int, RelationshipModel],
        node_id_remap: dict[int, int],
        rel_id_remap: dict[int, int],
        removed_node_ids: set[int],
        removed_rel_ids: set[int],
    ) -> bool:
        eid = self.entityId
        if self.entityType == "node":
            if eid in node_id_remap:
                eid = node_id_remap[eid]
            if eid in removed_node_ids:
                print(
                    f"WARNING: RemovePropertyAction on removed node #{eid}. Skip."
                )
                return False
            target = added_nodes.get(eid) or base_nodes.get(eid)
            if not target:
                print(f"WARNING: node #{eid} not found. Skip.")
                return False
            before_len = len(target.properties)
            target.properties = [
                p for p in target.properties if p.key != self.propertyKey
            ]
            after_len = len(target.properties)
            if before_len == after_len:
                print(
                    f"WARNING: node #{eid} has no property {self.propertyKey}. Skip."
                )
                return False
            return True

        elif self.entityType == "relationship":
            if eid in rel_id_remap:
                eid = rel_id_remap[eid]
            if eid in removed_rel_ids:
                print(
                    f"WARNING: RemovePropertyAction on removed relationship #{eid}. Skip."
                )
                return False
            target = added_rels.get(eid) or base_rels.get(eid)
            if not target:
                print(f"WARNING: relationship #{eid} not found. Skip.")
                return False
            before_len = len(target.properties)
            target.properties = [
                p for p in target.properties if p.key != self.propertyKey
            ]
            after_len = len(target.properties)
            if before_len == after_len:
                print(
                    f"WARNING: relationship #{eid} has no property {self.propertyKey}. Skip."
                )
                return False
            return True

        else:
            print(f"WARNING: invalid entityType {self.entityType}. Skip.")
            return False


class UpdateNodeLabelsAction(BaseModel):
    type: Literal["UpdateNodeLabels"]
    nodeId: int
    newLabels: list[str]

    @model_validator(mode="before")
    def default_type(cls, values: Self | dict):
        if isinstance(values, dict):
            values["type"] = "UpdateNodeLabels"
        else:
            values.type = "UpdateNodeLabels"
        return values


GraphAction = Union[
    AddNodeAction,
    RemoveNodeAction,
    AddRelationshipAction,
    RemoveRelationshipAction,
    AddPropertyAction,
    UpdatePropertyAction,
    RemovePropertyAction,
    UpdateNodeLabelsAction,
]


def apply_actions(
    base_graph: GraphModel, actions: list[GraphAction]
) -> GraphModel:
    """
    1) Re-map any 'AddNode'/'AddRelationship' IDs if they conflict with existing
    2) Sort actions in a stable order (add -> remove -> updates)
    3) Apply them in sequence
    4) Merge duplicates
    """
    nodes = {n.uniqueId: n for n in base_graph.nodes}
    relationships = {r.uniqueId: r for r in base_graph.relationships}

    existing_node_ids = set(nodes.keys())
    existing_rel_ids = set(relationships.keys())

    newly_added_nodes: dict[int, NodeModel] = {}
    newly_added_rels: dict[int, RelationshipModel] = {}

    remapped_node_ids = {}
    remapped_rel_ids = {}

    removed_node_ids = set()
    removed_rel_ids = set()

    # (A) First pass: check AddNode / AddRelationship to re-map IDs if needed
    for action in actions:
        if isinstance(action, AddNodeAction):
            for node in action.nodes:
                old_id = node.uniqueId
                if (
                    old_id in existing_node_ids
                    or old_id in remapped_node_ids
                ):
                    new_id = generate_new_id(
                        existing_node_ids, existing_rel_ids
                    )
                    remapped_node_ids[old_id] = new_id
                    existing_node_ids.add(new_id)
                else:
                    existing_node_ids.add(old_id)

        elif isinstance(action, AddRelationshipAction):
            for rel in action.relationships:
                old_rid = rel.uniqueId
                if (
                    old_rid in existing_rel_ids
                    or old_rid in remapped_rel_ids
                ):
                    new_rid = generate_new_id(
                        existing_node_ids, existing_rel_ids
                    )
                    remapped_rel_ids[old_rid] = new_rid
                    existing_rel_ids.add(new_rid)
                else:
                    existing_rel_ids.add(old_rid)

    # (B) Sort actions by priority
    sorted_actions: list[GraphAction] = sort_patch_actions(actions)

    # (C) Apply them in order
    for action in sorted_actions:
        if isinstance(action, AddNodeAction):
            for node in action.nodes:
                old_id = node.uniqueId
                if old_id in remapped_node_ids:
                    node.uniqueId = remapped_node_ids[old_id]
                newly_added_nodes[node.uniqueId] = node

        elif isinstance(action, AddRelationshipAction):
            for rel in action.relationships:
                old_rid = rel.uniqueId
                if old_rid in remapped_rel_ids:
                    rel.uniqueId = remapped_rel_ids[old_rid]

                start_id = rel.startNode.uniqueId
                if start_id in remapped_node_ids:
                    start_id = remapped_node_ids[start_id]
                    rel.startNode.uniqueId = start_id

                end_id = rel.endNode.uniqueId
                if end_id in remapped_node_ids:
                    end_id = remapped_node_ids[end_id]
                    rel.endNode.uniqueId = end_id

                # Validate if node is removed or not found
                if (start_id in removed_node_ids) or (
                    start_id not in nodes
                    and start_id not in newly_added_nodes
                ):
                    print(
                        f"WARNING: AddRelationship({rel.uniqueId}) start node {start_id} missing. Skip."
                    )
                    continue
                if (end_id in removed_node_ids) or (
                    end_id not in nodes and end_id not in newly_added_nodes
                ):
                    print(
                        f"WARNING: AddRelationship({rel.uniqueId}) end node {end_id} missing. Skip."
                    )
                    continue

                newly_added_rels[rel.uniqueId] = rel

        elif isinstance(action, RemoveNodeAction):
            for nid in action.nodeIds:
                if nid in remapped_node_ids:
                    nid = remapped_node_ids[nid]

                if nid in newly_added_nodes:
                    del newly_added_nodes[nid]
                    removed_node_ids.add(nid)
                elif nid in nodes:
                    del nodes[nid]
                    removed_node_ids.add(nid)
                    # Remove relationships referencing the removed node
                    for rid in list(relationships.keys()):
                        rel = relationships[rid]
                        if (
                            rel.startNode.uniqueId == nid
                            or rel.endNode.uniqueId == nid
                        ):
                            del relationships[rid]
                            removed_rel_ids.add(rid)
                    for rid in list(newly_added_rels.keys()):
                        rel = newly_added_rels[rid]
                        if (
                            rel.startNode.uniqueId == nid
                            or rel.endNode.uniqueId == nid
                        ):
                            del newly_added_rels[rid]
                            removed_rel_ids.add(rid)
                else:
                    print(
                        f"WARNING: RemoveNodeAction: node {nid} not found. Skip."
                    )

        elif isinstance(action, RemoveRelationshipAction):
            for rid in action.relationshipIds:
                if rid in remapped_rel_ids:
                    rid = remapped_rel_ids[rid]

                if rid in newly_added_rels:
                    del newly_added_rels[rid]
                    removed_rel_ids.add(rid)
                elif rid in relationships:
                    del relationships[rid]
                    removed_rel_ids.add(rid)
                else:
                    print(
                        f"WARNING: RemoveRelationshipAction: relationship {rid} not found. Skip."
                    )

        elif isinstance(action, UpdateNodeLabelsAction):
            nid = action.nodeId
            if nid in remapped_node_ids:
                nid = remapped_node_ids[nid]
            if nid in removed_node_ids:
                print(
                    f"WARNING: node {nid} was removed, can't update labels. Skip."
                )
                continue
            node_obj = newly_added_nodes.get(nid) or nodes.get(nid)
            if not node_obj:
                print(
                    f"WARNING: UpdateNodeLabelsAction: node {nid} not found. Skip."
                )
                continue
            node_obj.labels = action.newLabels

        elif isinstance(action, AddPropertyAction):
            action.apply_add_property(
                base_nodes=nodes,
                added_nodes=newly_added_nodes,
                base_rels=relationships,
                added_rels=newly_added_rels,
                node_id_remap=remapped_node_ids,
                rel_id_remap=remapped_rel_ids,
                removed_node_ids=removed_node_ids,
                removed_rel_ids=removed_rel_ids,
            )

        elif isinstance(action, UpdatePropertyAction):
            action.apply_update_property(
                base_nodes=nodes,
                added_nodes=newly_added_nodes,
                base_rels=relationships,
                added_rels=newly_added_rels,
                node_id_remap=remapped_node_ids,
                rel_id_remap=remapped_rel_ids,
                removed_node_ids=removed_node_ids,
                removed_rel_ids=removed_rel_ids,
            )

        elif isinstance(action, RemovePropertyAction):
            action.apply_remove_property(
                base_nodes=nodes,
                added_nodes=newly_added_nodes,
                base_rels=relationships,
                added_rels=newly_added_rels,
                node_id_remap=remapped_node_ids,
                rel_id_remap=remapped_rel_ids,
                removed_node_ids=removed_node_ids,
                removed_rel_ids=removed_rel_ids,
            )

        else:
            print(
                f"WARNING: Unknown action type {action.type} encountered. Skipped."
            )

    # (D) Merge newly-added into base
    nodes.update(newly_added_nodes)
    relationships.update(newly_added_rels)

    # (E) Construct new graph, unify duplicates
    updated_graph = GraphModel(
        nodes=list(nodes.values()),
        relationships=list(relationships.values()),
    )
    updated_graph.resolve_merge_conflicts()

    return updated_graph


def generate_new_id(
    existing_node_ids: set[int], existing_rel_ids: set[int]
) -> int:
    used = existing_node_ids.union(existing_rel_ids)
    candidate = 1
    while candidate in used:
        candidate += 1
    return candidate


def sort_patch_actions(actions: list[GraphAction]) -> list[GraphAction]:
    """
    Sort the actions in a stable, logical order to avoid referencing missing entities.
    Priority is:
      1) AddNode
      2) AddRelationship
      3) RemoveNode
      4) RemoveRelationship
      5) UpdateNodeLabels
      6) AddProperty
      7) UpdateProperty
      8) RemoveProperty
    """
    PRIORITY = {
        "AddNode": 1,
        "AddRelationship": 2,
        "RemoveNode": 3,
        "RemoveRelationship": 4,
        "UpdateNodeLabels": 5,
        "AddProperty": 6,
        "UpdateProperty": 7,
        "RemoveProperty": 8,
    }

    def action_priority(a: GraphAction) -> int:
        return PRIORITY[a.type]

    return sorted(actions, key=action_priority)
