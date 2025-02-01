from copy import deepcopy
import logging
from collections import defaultdict
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


A = TypeVar("A", bound=ActionTypes)


class GraphActionBase(BaseModel, Generic[A]):
    type: A

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


class AddRelationshipAction(GraphActionBase[Literal["AddRelationship"]]):
    relationships: list[RelationshipModel]


class AddPropertyAction(GraphActionBase[Literal["AddProperty"]]):
    entityId: int
    property: PropertyModel


class RemoveNodeAction(GraphActionBase[Literal["RemoveNode"]]):
    nodeIds: list[int]


class RemoveRelationshipAction(
    GraphActionBase[Literal["RemoveRelationship"]]
):
    relationshipIds: list[int]


class RemovePropertyAction(GraphActionBase[Literal["RemoveProperty"]]):
    entityId: int
    propertyKey: str


class UpdateNodeLabelsAction(GraphActionBase[Literal["UpdateNodeLabels"]]):
    nodeId: int
    newLabels: list[str]


class UpdateRelationshipTypeAction(
    GraphActionBase[Literal["UpdateRelationshipType"]]
):
    relationshipId: int
    newType: str


class UpdatePropertyAction(GraphActionBase[Literal["UpdateProperty"]]):
    entityId: int
    propertyKey: str
    newValue: PropertyType


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
        if isinstance(action, AddNodeAction):
            _ns: defaultdict[int, list[NodeModel]] = defaultdict(list)
            for _n in action.nodes:
                _existing_node: Optional[NodeModel] = graph_nodes.get(
                    _n.uniqueId
                )
                if _existing_node is not None:
                    _ns[_n.uniqueId].append(_existing_node)
                _ns[_n.uniqueId].append(_n)

            for _nid, _rl in _ns.items():
                if not _rl:
                    continue
                if len(_rl) > 1:
                    logger.warning(
                        f"AddNodeAction: Node {_nid} added multiple times. Only the last one will be kept."
                    )
                graph_nodes[_nid] = NodeModel.merge_with_id(
                    entities=_rl, uniqueId=_nid
                )

        elif isinstance(action, AddRelationshipAction):
            _rs: defaultdict[int, list[RelationshipModel]] = defaultdict(
                list
            )
            for _r in action.relationships:
                _existing_rel: Optional[RelationshipModel] = (
                    graph_relationships.get(_r.uniqueId)
                )
                if _existing_rel is not None:
                    _rs[_r.uniqueId].append(_existing_rel)
                _rs[_r.uniqueId].append(_r)

            for _rid, _rl in _rs.items():
                if not _rl:
                    continue
                if len(_rl) > 1:
                    logger.warning(
                        f"AddRelationshipAction: Relationship {_rid} added multiple times. Only the last one will be kept."
                    )
                graph_relationships[_rid] = RelationshipModel.merge_with_id(
                    entities=_rl, uniqueId=_rid
                )

        elif isinstance(action, AddPropertyAction):
            _eid = action.entityId
            _p = action.property
            if _eid in graph_nodes:
                _n = graph_nodes[_eid]
                if _eid in graph_relationships:
                    logger.warning(
                        f"AddPropertyAction: Both node {_eid} and relationship {_eid} found. Node will be used."
                    )
                _existing_p = next(
                    (p for p in _n.properties if p.k == _p.k),
                    None,
                )
                if _existing_p is not None:
                    logger.warning(
                        f"AddPropertyAction: Property {_existing_p.k} already exists in node {_eid}. Overwriting."
                    )
                    _n.properties.remove(_existing_p)
                    continue
                _n.properties.append(_p)
            elif _eid in graph_relationships:
                _r = graph_relationships[_eid]
                _existing_p = next(
                    (p for p in _r.properties if p.k == _p.k),
                    None,
                )
                if _existing_p is not None:
                    logger.warning(
                        f"AddPropertyAction: Property {_existing_p.k} already exists in relationship {_eid}. Overwriting."
                    )
                    _r.properties.remove(_existing_p)
                    continue
                _r.properties.append(_p)
            else:
                logger.warning(
                    f"AddPropertyAction: Entity {_eid} not found. Skip."
                )

        elif isinstance(action, RemoveNodeAction):
            for _nid in action.nodeIds:
                if _nid in graph_nodes:
                    del graph_nodes[_nid]
                else:
                    logger.warning(
                        f"RemoveNodeAction: node {_nid} not found. Skip."
                    )

        elif isinstance(action, RemoveRelationshipAction):
            for _rid in action.relationshipIds:
                if _rid in graph_relationships:
                    del graph_relationships[_rid]
                else:
                    logger.warning(
                        f"RemoveRelationshipAction: relationship {_rid} not found. Skip."
                    )

        elif isinstance(action, RemovePropertyAction):
            _eid = action.entityId
            _pk = action.propertyKey
            if _eid in graph_nodes:
                _n = graph_nodes[_eid]
                if _eid in graph_relationships:
                    logger.warning(
                        f"RemovePropertyAction: Both node {_eid} and relationship {_eid} found. Node will be used."
                    )
                _existing_p = next(
                    (p for p in _n.properties if p.k == _pk),
                    None,
                )
                if _existing_p is not None:
                    _n.properties.remove(_existing_p)
                else:
                    logger.warning(
                        f"RemovePropertyAction: Property {_pk} not found in node {_eid}. Skip."
                    )
            elif _eid in graph_relationships:
                _r = graph_relationships[_eid]
                _existing_p = next(
                    (p for p in _r.properties if p.k == _pk),
                    None,
                )
                if _existing_p is not None:
                    _r.properties.remove(_existing_p)
                else:
                    logger.warning(
                        f"RemovePropertyAction: Property {_pk} not found in relationship {_eid}. Skip."
                    )
            else:
                logger.warning(
                    f"RemovePropertyAction: Entity {_eid} not found. Skip."
                )

        elif isinstance(action, UpdateNodeLabelsAction):
            _nid = action.nodeId
            _n = graph_nodes.get(_nid)
            if _n is None:
                logger.warning(
                    f"UpdateNodeLabelsAction: node {_nid} not found. Skip."
                )
                continue
            _n.labels = action.newLabels
        elif isinstance(action, UpdateRelationshipTypeAction):
            _rid = action.relationshipId
            _r = graph_relationships.get(_rid)
            if _r is None:
                logger.warning(
                    f"UpdateRelationshipTypeAction: relationship {_rid} not found. Skip."
                )
                continue
            _r.type = action.newType

        elif isinstance(action, UpdatePropertyAction):
            _eid = action.entityId
            _pk = action.propertyKey
            _nv = action.newValue
            if _eid in graph_nodes:
                _n = graph_nodes[_eid]
                if _eid in graph_relationships:
                    logger.warning(
                        f"UpdatePropertyAction: Both node {_eid} and relationship {_eid} found. Node will be used."
                    )
                _existing_p = next(
                    (p for p in _n.properties if p.k == _pk),
                    None,
                )
                if _existing_p is not None:
                    _existing_p.v = _nv
                else:
                    logger.warning(
                        f"UpdatePropertyAction: Property {_pk} not found in node {_eid}. Creating new property."
                    )
                    _n.properties.append(PropertyModel(k=_pk, v=_nv))
            elif _eid in graph_relationships:
                _r = graph_relationships[_eid]
                _existing_p = next(
                    (p for p in _r.properties if p.k == _pk),
                    None,
                )
                if _existing_p is not None:
                    _existing_p.v = _nv
                else:
                    logger.warning(
                        f"UpdatePropertyAction: Property {_pk} not found in relationship {_eid}. Creating new property."
                    )
                    _r.properties.append(PropertyModel(k=_pk, v=_nv))
            else:
                logger.warning(
                    f"UpdatePropertyAction: Entity {_eid} not found. Skip."
                )

    return GraphModel(
        nodes=list(graph_nodes.values()),
        relationships=list(graph_relationships.values()),
    )
