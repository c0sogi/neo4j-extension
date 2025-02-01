import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from typing import (
    Generic,
    Literal,
    Optional,
    Self,
    TypeAlias,
    TypeVar,
    Union,
    get_args,
)

from pydantic import BaseModel, model_validator

from .pydantic_model import (
    GraphModel,
    NodeModel,
    PropertyModel,
    PropertyType,
    RelationshipModel,
)

logger = logging.getLogger(__name__)
ActionTypes: TypeAlias = Literal[
    "AddNode",
    "RemoveNode",
    "AddRelationship",
    "RemoveRelationship",
    "AddProperty",
    "UpdateProperty",
    "RemoveProperty",
    "UpdateNodeLabels",
    "UpdateRelationshipType",
]
ActionT = TypeVar("ActionT", bound=ActionTypes)


class GraphActionBase(BaseModel, Generic[ActionT], ABC):
    type: ActionT

    @abstractmethod
    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None: ...

    @model_validator(mode="before")
    def default_type(cls, values: Union[Self, dict]):
        try:
            (type,) = get_args(cls.__pydantic_fields__["type"].annotation)
            if isinstance(values, dict):
                values["type"] = type
            if isinstance(values, GraphActionBase):
                values.type = type
            return values
        except Exception as e:
            logger.error(
                f"[{cls.__class__}] Failed to set default type: {e}"
            )


class AddNodeAction(GraphActionBase[Literal["AddNode"]]):
    nodes: list[NodeModel]

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        nodes: defaultdict[int, list[NodeModel]] = defaultdict(list)
        for node in self.nodes:
            existing_node: Optional[NodeModel] = graph_nodes.get(
                node.uniqueId
            )
            if existing_node is not None:
                nodes[node.uniqueId].append(existing_node)
            nodes[node.uniqueId].append(node)

        for node_id, node_list in nodes.items():
            if not node_list:
                return
            if len(node_list) > 1:
                logger.warning(
                    f"AddNodeAction: Node {node_id} added multiple times. Only the last one will be kept."
                )
            graph_nodes[node_id] = NodeModel.merge_with_id(
                entities=node_list, uniqueId=node_id
            )


class AddRelationshipAction(GraphActionBase[Literal["AddRelationship"]]):
    relationships: list[RelationshipModel]

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        relationships: defaultdict[int, list[RelationshipModel]] = (
            defaultdict(list)
        )
        for relationship in self.relationships:
            existing_relationship: Optional[RelationshipModel] = (
                graph_relationships.get(relationship.uniqueId)
            )
            if existing_relationship is not None:
                relationships[relationship.uniqueId].append(
                    existing_relationship
                )
            relationships[relationship.uniqueId].append(relationship)

        for relationship_id, relationship_list in relationships.items():
            if not relationship_list:
                return
            if len(relationship_list) > 1:
                logger.warning(
                    f"AddRelationshipAction: Relationship {relationship_id} added multiple times. Only the last one will be kept."
                )
            graph_relationships[relationship_id] = (
                RelationshipModel.merge_with_id(
                    entities=relationship_list, uniqueId=relationship_id
                )
            )


class AddPropertyAction(GraphActionBase[Literal["AddProperty"]]):
    entityId: int
    property: PropertyModel

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        entity_id: int = self.entityId
        property: PropertyModel = self.property
        if entity_id in graph_nodes:
            node: NodeModel = graph_nodes[entity_id]
            if entity_id in graph_relationships:
                logger.warning(
                    f"AddPropertyAction: Both node {entity_id} and relationship {entity_id} found. Node will be used."
                )
            existing_property: Optional[PropertyModel] = next(
                (p for p in node.properties if p.k == property.k),
                None,
            )
            if existing_property is not None:
                logger.warning(
                    f"AddPropertyAction: Property {existing_property.k} already exists in node {entity_id}. Overwriting."
                )
                node.properties.remove(existing_property)
                return
            node.properties.append(property)
        elif entity_id in graph_relationships:
            relationship: RelationshipModel = graph_relationships[entity_id]
            existing_property = next(
                (p for p in relationship.properties if p.k == property.k),
                None,
            )
            if existing_property is not None:
                logger.warning(
                    f"AddPropertyAction: Property {existing_property.k} already exists in relationship {entity_id}. Overwriting."
                )
                relationship.properties.remove(existing_property)
                return
            relationship.properties.append(property)
        else:
            logger.warning(
                f"AddPropertyAction: Entity {entity_id} not found. Skip."
            )


class RemoveNodeAction(GraphActionBase[Literal["RemoveNode"]]):
    nodeIds: list[int]

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        for node_id in self.nodeIds:
            if node_id in graph_nodes:
                del graph_nodes[node_id]
            else:
                logger.warning(
                    f"RemoveNodeAction: node {node_id} not found. Skip."
                )


class RemoveRelationshipAction(
    GraphActionBase[Literal["RemoveRelationship"]]
):
    relationshipIds: list[int]

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        for relationship_id in self.relationshipIds:
            if relationship_id in graph_relationships:
                del graph_relationships[relationship_id]
            else:
                logger.warning(
                    f"RemoveRelationshipAction: relationship {relationship_id} not found. Skip."
                )


class RemovePropertyAction(GraphActionBase[Literal["RemoveProperty"]]):
    entityId: int
    propertyKey: str

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        entity_id: int = self.entityId
        property_key: str = self.propertyKey
        if entity_id in graph_nodes:
            node: NodeModel = graph_nodes[entity_id]
            if entity_id in graph_relationships:
                logger.warning(
                    f"RemovePropertyAction: Both node {entity_id} and relationship {entity_id} found. Node will be used."
                )
            existing_property: Optional[PropertyModel] = next(
                (p for p in node.properties if p.k == property_key),
                None,
            )
            if existing_property is not None:
                node.properties.remove(existing_property)
            else:
                logger.warning(
                    f"RemovePropertyAction: Property {property_key} not found in node {entity_id}. Skip."
                )
        elif entity_id in graph_relationships:
            relationship: RelationshipModel = graph_relationships[entity_id]
            existing_property = next(
                (p for p in relationship.properties if p.k == property_key),
                None,
            )
            if existing_property is not None:
                relationship.properties.remove(existing_property)
            else:
                logger.warning(
                    f"RemovePropertyAction: Property {property_key} not found in relationship {entity_id}. Skip."
                )
        else:
            logger.warning(
                f"RemovePropertyAction: Entity {entity_id} not found. Skip."
            )


class UpdateNodeLabelsAction(GraphActionBase[Literal["UpdateNodeLabels"]]):
    nodeId: int
    newLabels: list[str]

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        node_id = self.nodeId
        node: Optional[NodeModel] = graph_nodes.get(node_id)
        if node is None:
            logger.warning(
                f"UpdateNodeLabelsAction: node {node_id} not found. Skip."
            )
            return
        node.labels = self.newLabels


class UpdateRelationshipTypeAction(
    GraphActionBase[Literal["UpdateRelationshipType"]]
):
    relationshipId: int
    newType: str

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        relationship_id = self.relationshipId
        relationship: Optional[RelationshipModel] = graph_relationships.get(
            relationship_id
        )
        if relationship is None:
            logger.warning(
                f"UpdateRelationshipTypeAction: relationship {relationship_id} not found. Skip."
            )
            return
        relationship.type = self.newType


class UpdatePropertyAction(GraphActionBase[Literal["UpdateProperty"]]):
    entityId: int
    propertyKey: str
    newValue: PropertyType

    def apply_action(
        self,
        graph_nodes: dict[int, NodeModel],
        graph_relationships: dict[int, RelationshipModel],
    ) -> None:
        entity_id = self.entityId
        property_key = self.propertyKey
        property_value = self.newValue
        if entity_id in graph_nodes:
            node: NodeModel = graph_nodes[entity_id]
            if entity_id in graph_relationships:
                logger.warning(
                    f"UpdatePropertyAction: Both node {entity_id} and relationship {entity_id} found. Node will be used."
                )
            existing_property = next(
                (p for p in node.properties if p.k == property_key),
                None,
            )
            if existing_property is not None:
                existing_property.v = property_value
            else:
                logger.warning(
                    f"UpdatePropertyAction: Property {property_key} not found in node {entity_id}. Creating new property."
                )
                node.properties.append(
                    PropertyModel(k=property_key, v=property_value)
                )
        elif entity_id in graph_relationships:
            relationship = graph_relationships[entity_id]
            existing_property = next(
                (p for p in relationship.properties if p.k == property_key),
                None,
            )
            if existing_property is not None:
                existing_property.v = property_value
            else:
                logger.warning(
                    f"UpdatePropertyAction: Property {property_key} not found in relationship {entity_id}. Creating new property."
                )
                relationship.properties.append(
                    PropertyModel(k=property_key, v=property_value)
                )
        else:
            logger.warning(
                f"UpdatePropertyAction: Entity {entity_id} not found. Skip."
            )


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
    graph: GraphModel, actions: list[GraphAction]
) -> GraphModel:
    graph_nodes: dict[int, NodeModel] = {
        node.uniqueId: deepcopy(node) for node in graph.nodes
    }
    graph_relationships: dict[int, RelationshipModel] = {
        rel.uniqueId: deepcopy(rel) for rel in graph.relationships
    }
    for action in actions:
        action.apply_action(
            graph_nodes=graph_nodes, graph_relationships=graph_relationships
        )
    return GraphModel(
        nodes=list(graph_nodes.values()),
        relationships=list(graph_relationships.values()),
    )
