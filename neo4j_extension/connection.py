import logging
from dataclasses import dataclass, field
from functools import wraps
from os import environ
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Concatenate,
    Final,
    Iterable,
    Literal,
    LiteralString,
    Optional,
    ParamSpec,
    Self,
    TypeAlias,
    TypedDict,
    TypeVar,
    cast,
)

import neo4j
import neo4j.auth_management
from neo4j import (
    AsyncDriver,
    AsyncGraphDatabase,
    AsyncManagedTransaction,
    AsyncSession,
    Driver,
    GraphDatabase,
    ManagedTransaction,
    Session,
)

from ._utils import _escape_identifier
from .graph import Graph, Node, Relationship

if TYPE_CHECKING:
    import ssl

    class SessionKwargs(TypedDict, total=False):
        connection_acquisition_timeout: float
        max_transaction_retry_time: float
        database: Optional[str]
        fetch_size: int
        impersonated_user: Optional[str]
        bookmarks: Optional[Iterable[str] | neo4j.api.Bookmarks]
        default_access_mode: str
        bookmark_manager: Optional[neo4j.api.BookmarkManager]
        auth: neo4j.api._TAuth
        notifications_min_severity: Optional[
            neo4j._api.T_NotificationMinimumSeverity
        ]
        notifications_disabled_categories: Optional[
            Iterable[neo4j._api.T_NotificationDisabledCategory]
        ]
        notifications_disabled_classifications: Optional[
            Iterable[neo4j._api.T_NotificationDisabledCategory]
        ]
        # undocumented/unsupported options
        # they may be change or removed any time without prior notice
        initial_retry_delay: float
        retry_delay_multiplier: float
        retry_delay_jitter_factor: float

    class DriverKwargs(TypedDict, total=False):
        uri: str
        auth: neo4j.api._TAuth | neo4j.auth_management.AuthManager
        max_connection_lifetime: float
        liveness_check_timeout: Optional[float]
        max_connection_pool_size: int
        connection_timeout: float
        trust: Literal[
            "TRUST_ALL_CERTIFICATES", "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"
        ]
        resolver: (
            Callable[
                [neo4j.addressing.Address],
                Iterable[neo4j.addressing.Address],
            ]
            | Callable[
                [neo4j.addressing.Address],
                Iterable[neo4j.addressing.Address],
            ]
        )
        encrypted: bool
        trusted_certificates: neo4j.security.TrustStore
        client_certificate: Optional[
            neo4j.security.ClientCertificate
            | neo4j.security.ClientCertificateProvider
        ]
        ssl_context: Optional[ssl.SSLContext]
        user_agent: str
        keep_alive: bool
        notifications_min_severity: Optional[
            neo4j._api.T_NotificationMinimumSeverity
        ]
        notifications_disabled_categories: Optional[
            Iterable[neo4j._api.T_NotificationDisabledCategory]
        ]
        notifications_disabled_classifications: Optional[
            Iterable[neo4j._api.T_NotificationDisabledCategory]
        ]
        warn_notification_severity: Optional[
            neo4j._api.T_NotificationMinimumSeverity
        ]
        telemetry_disabled: bool
        connection_acquisition_timeout: float
        max_transaction_retry_time: float
        initial_retry_delay: float
        retry_delay_multiplier: float
        retry_delay_jitter_factor: float
        database: Optional[str]
        fetch_size: int
        impersonated_user: Optional[str]
        bookmark_manager: Optional[neo4j.api.BookmarkManager]

    class AsyncDriverKwargs(TypedDict, total=False):
        uri: str
        auth: neo4j.api._TAuth | neo4j.auth_management.AsyncAuthManager
        max_connection_lifetime: float
        liveness_check_timeout: Optional[float]
        max_connection_pool_size: int
        connection_timeout: float
        trust: Literal[
            "TRUST_ALL_CERTIFICATES", "TRUST_SYSTEM_CA_SIGNED_CERTIFICATES"
        ]
        resolver: (
            Callable[
                [neo4j.addressing.Address],
                Iterable[neo4j.addressing.Address],
            ]
            | Callable[
                [neo4j.addressing.Address],
                Iterable[neo4j.addressing.Address],
            ]
        )
        encrypted: bool
        trusted_certificates: neo4j.security.TrustStore
        client_certificate: Optional[
            neo4j.security.ClientCertificate
            | neo4j.security.ClientCertificateProvider
        ]
        ssl_context: Optional[ssl.SSLContext]
        user_agent: str
        keep_alive: bool
        notifications_min_severity: Optional[
            neo4j._api.T_NotificationMinimumSeverity
        ]
        notifications_disabled_categories: Optional[
            Iterable[neo4j._api.T_NotificationDisabledCategory]
        ]
        notifications_disabled_classifications: Optional[
            Iterable[neo4j._api.T_NotificationDisabledCategory]
        ]
        warn_notification_severity: Optional[
            neo4j._api.T_NotificationMinimumSeverity
        ]
        telemetry_disabled: bool
        connection_acquisition_timeout: float
        max_transaction_retry_time: float
        initial_retry_delay: float
        retry_delay_multiplier: float
        retry_delay_jitter_factor: float
        database: Optional[str]
        fetch_size: int
        impersonated_user: Optional[str]
        bookmark_manager: Optional[neo4j.api.BookmarkManager]

else:
    SessionKwargs: TypeAlias = dict
    DriverKwargs: TypeAlias = dict
    AsyncDriverKwargs: TypeAlias = dict

NODE_PROPERTIES_QUERY: Final = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" 
    AND elementType = "node"
    AND NOT label IN $EXCLUDED_LABELS
WITH label AS nodeLabels, collect({property: property, type: type}) AS properties
RETURN {labels: nodeLabels, properties: properties} AS output
"""

REL_PROPERTIES_QUERY: Final = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" 
    AND elementType = "relationship"
    AND NOT label IN $EXCLUDED_RELS
WITH label AS nodeLabels, collect({property: property, type: type}) AS properties
RETURN {type: nodeLabels, properties: properties} AS output
"""

REL_QUERY: Final = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE type = "RELATIONSHIP" 
    AND elementType = "node"
UNWIND other AS other_node
WITH * 
WHERE NOT label IN $EXCLUDED_LABELS
    AND NOT other_node IN $EXCLUDED_LABELS
RETURN {start: label, type: property, end: toString(other_node)} AS output
"""

INDEX_RES_QUERY: Final = """
CALL apoc.schema.nodes() 
YIELD label, properties, type, size, valuesSelectivity
WHERE type = 'RANGE'
RETURN *, size * valuesSelectivity as distinctValues
"""


ENV_NEO4J_HOST: str = environ.get("NEO4J_HOST", "localhost")
ENV_NEO4J_USER: str = environ.get("NEO4J_USER", "neo4j")
ENV_NEO4J_PASSWORD: str = environ.get("NEO4J_PASSWORD", "")
ENV_NEO4J_PORT: str = environ.get("NEO4J_PORT", "7474")
ENV_NEO4J_BOLT_PORT: str = environ.get("NEO4J_BOLT_PORT", "7687")

P = ParamSpec("P")
T = TypeVar("T")
Neo4j = TypeVar("Neo4j", bound="Neo4jConnection")

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class Prop(TypedDict):
    property: str
    type: str


class Rel(TypedDict):
    start: str
    type: str
    end: str


class StructuredSchemaMetadata(TypedDict):
    constraint: list[dict[str, Any]]
    index: list[dict[str, Any]]


class GraphSchema(TypedDict):
    node_props: dict[str, list[Prop]]
    rel_props: dict[str, list[Prop]]
    relationships: list[Rel]
    metadata: StructuredSchemaMetadata


class with_session:
    @staticmethod
    def scope(
        method: Callable[Concatenate[Neo4j, Session, P], T]
    ) -> Callable[Concatenate[Neo4j, P], T]:
        @wraps(method)
        def wrapper(self: Neo4j, *args: P.args, **kwargs: P.kwargs) -> T:
            with self.connection.session(**self.session_kwargs) as session:
                return method(self, session, *args, **kwargs)

        return wrapper

    @staticmethod
    def readwrite_transaction(
        method: Callable[Concatenate[Neo4j, ManagedTransaction, P], T]
    ) -> Callable[Concatenate[Neo4j, P], T]:
        @wraps(method)
        def wrapper(self: Neo4j, *args: P.args, **kwargs: P.kwargs) -> T:
            with self.connection.session(**self.session_kwargs) as session:
                return session.execute_write(
                    lambda tx: method(self, tx, *args, **kwargs)
                )

        return wrapper

    @staticmethod
    def readonly_transaction(
        method: Callable[Concatenate[Neo4j, ManagedTransaction, P], T]
    ) -> Callable[Concatenate[Neo4j, P], T]:
        @wraps(method)
        def wrapper(self: Neo4j, *args: P.args, **kwargs: P.kwargs) -> T:
            with self.connection.session(**self.session_kwargs) as session:
                return session.execute_read(
                    lambda tx: method(self, tx, *args, **kwargs)
                )

        return wrapper


class with_async_session:
    @staticmethod
    def scope(
        method: Callable[Concatenate[Neo4j, AsyncSession, P], Awaitable[T]]
    ) -> Callable[Concatenate[Neo4j, P], Awaitable[T]]:
        @wraps(method)
        async def wrapper(
            self: Neo4j, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            async with (await self.aconnection).session(
                **self.session_kwargs
            ) as session:
                return await method(self, session, *args, **kwargs)

        return wrapper

    @staticmethod
    def readwrite_transaction(
        method: Callable[
            Concatenate[Neo4j, AsyncManagedTransaction, P], Awaitable[T]
        ]
    ) -> Callable[Concatenate[Neo4j, P], Awaitable[T]]:
        @wraps(method)
        async def wrapper(
            self: Neo4j, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            async with (await self.aconnection).session(
                **self.session_kwargs
            ) as session:
                return await session.execute_write(
                    lambda tx: method(self, tx, *args, **kwargs)
                )

        return wrapper

    @staticmethod
    def readonly_transaction(
        method: Callable[
            Concatenate[Neo4j, AsyncManagedTransaction, P], Awaitable[T]
        ]
    ) -> Callable[Concatenate[Neo4j, P], Awaitable[T]]:
        @wraps(method)
        async def wrapper(
            self: Neo4j, *args: P.args, **kwargs: P.kwargs
        ) -> T:
            async with (await self.aconnection).session(
                **self.session_kwargs
            ) as session:
                return await session.execute_read(
                    lambda tx: method(self, tx, *args, **kwargs)
                )

        return wrapper


@dataclass
class Neo4jConnection:
    """Neo4j Connection

    Attributes:
        host: str
        port: str
        password: str
        user: str
        protocol: str
        driver: Optional[Driver]
    """

    host: str = ENV_NEO4J_HOST
    port: str = ENV_NEO4J_BOLT_PORT
    password: str = ENV_NEO4J_PASSWORD
    user: str = ENV_NEO4J_USER
    protocol: str = "neo4j"
    driver: Optional[Driver] = None
    async_driver: Optional[AsyncDriver] = None
    driver_kwargs: DriverKwargs = field(default_factory=DriverKwargs)
    async_driver_kwargs: AsyncDriverKwargs = field(
        default_factory=AsyncDriverKwargs
    )
    session_kwargs: SessionKwargs = field(default_factory=SessionKwargs)

    def connect(self) -> Driver:
        driver_kwargs: DriverKwargs = self.driver_kwargs.copy()
        if "uri" not in driver_kwargs:
            driver_kwargs["uri"] = self.uri
        if "auth" not in driver_kwargs:
            driver_kwargs["auth"] = self.auth

        logger.info(f"neo4j::connecting to `{self.uri}` ...")
        self.driver = GraphDatabase.driver(**driver_kwargs)
        self.driver.verify_connectivity()
        logger.info(f"neo4j::connected to `{self.uri}`")
        return self.driver

    async def aconnect(self) -> AsyncDriver:
        async_driver_kwargs: AsyncDriverKwargs = (
            self.async_driver_kwargs.copy()
        )
        if "uri" not in async_driver_kwargs:
            async_driver_kwargs["uri"] = self.uri
        if "auth" not in async_driver_kwargs:
            async_driver_kwargs["auth"] = self.auth

        logger.info(f"neo4j::connecting to `{self.uri}` ...")
        self.async_driver = AsyncGraphDatabase.driver(**async_driver_kwargs)
        await self.async_driver.verify_connectivity()
        logger.info(f"neo4j::connected to `{self.uri}`")
        return self.async_driver

    @property
    def connection(self) -> Driver:
        if self.driver is None:
            return self.connect()
        return self.driver

    @property
    async def aconnection(self) -> AsyncDriver:
        if self.async_driver is None:
            return await self.aconnect()
        return self.async_driver

    @property
    def uri(self) -> str:
        return f"{self.protocol}://{self.host}:{self.port}"

    @property
    def auth(self) -> tuple[str, str]:
        return (self.user, self.password)

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()
            self.driver = None
            logger.info(f"neo4j::closed connection to `{self.uri}`")

    async def aclose(self) -> None:
        if self.async_driver is not None:
            await self.async_driver.close()
            self.async_driver = None
            logger.info(f"neo4j::closed connection to `{self.uri}`")

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    async def __aenter__(self) -> Self:
        await self.aconnect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    @with_session.readwrite_transaction
    def clear_all(self, tx: ManagedTransaction) -> None:
        """Clear all data in the database"""
        tx.run("MATCH (n) DETACH DELETE n")

    @with_async_session.readwrite_transaction
    async def aclear_all(self, tx: AsyncManagedTransaction) -> None:
        """Clear all data in the database"""
        await tx.run("MATCH (n) DETACH DELETE n")

    @property
    @with_session.scope
    def graph_schema(
        self,
        session: Session,
        excluded_labels: Optional[list[str]] = None,
        excluded_rels: Optional[list[str]] = None,
    ) -> GraphSchema:
        if excluded_labels is None:
            excluded_labels = ["_Bloom_Perspective_", "_Bloom_Scene_"]
        if excluded_rels is None:
            excluded_rels = ["_Bloom_HAS_SCENE_"]

        def run_query(
            query: str, params: Optional[dict[str, Any]] = None
        ) -> list[dict[str, Any]]:
            result = session.run(cast(LiteralString, query), params or {})
            return [record.data() for record in result]

        node_properties_res = run_query(
            query=NODE_PROPERTIES_QUERY,
            params={"EXCLUDED_LABELS": excluded_labels},
        )
        rel_properties_res = run_query(
            query=REL_PROPERTIES_QUERY,
            params={"EXCLUDED_RELS": excluded_rels},
        )
        relationships_res = run_query(
            query=REL_QUERY,
            params={"EXCLUDED_LABELS": excluded_labels},
        )

        try:
            constraint_res: list[dict[str, Any]] = run_query(
                "SHOW CONSTRAINTS"
            )
        except neo4j.exceptions.Neo4jError as e:
            logger.warning(f"Cannot read constraints: {e}")
            constraint_res = []

        try:
            index_res = run_query(INDEX_RES_QUERY)
        except neo4j.exceptions.Neo4jError as e:
            logger.warning(f"Cannot read indexes: {e}")
            index_res = []

        structured_schema: GraphSchema = {
            "node_props": {
                item["output"]["labels"]: item["output"]["properties"]
                for item in node_properties_res
            },
            "rel_props": {
                item["output"]["type"]: item["output"]["properties"]
                for item in rel_properties_res
            },
            "relationships": [item["output"] for item in relationships_res],
            "metadata": {
                "constraint": constraint_res,
                "index": index_res,
            },
        }
        return structured_schema

    @property
    def formatted_graph_schema(self) -> str:
        return self.format_graph_schema(self.graph_schema)

    @staticmethod
    def format_graph_schema(graph_schema: GraphSchema) -> str:
        lines: list[str] = []

        lines.append("### Node properties")
        node_props: dict[str, list[Prop]] = graph_schema.get(
            "node_props", {}
        )
        for label, props in node_props.items():
            lines.append(f"- {label}")
            for p in props:
                lines.append(f"  * {p['property']}: {p['type']}")

        lines.append("\n### Relationship properties")
        rel_props: dict[str, list[Prop]] = graph_schema.get("rel_props", {})
        for rtype, rprops in rel_props.items():
            lines.append(f"- {rtype}")
            for rp in rprops:
                lines.append(f"  * {rp['property']}: {rp['type']}")

        lines.append("\n### Relationships")
        rels = graph_schema.get("relationships", [])
        for rel_dict in rels:
            lines.append(
                f"- (:{rel_dict['start']})-[:{rel_dict['type']}]->(:{rel_dict['end']})"
            )
        return "\n".join(lines)

    def _do_upsert_node(self, tx: ManagedTransaction, node: Node) -> dict:
        """Merge node based on element_id in a transaction,
        set all properties as new if node does not exist.
        Use temporary label "NoLabel" if no label is present."""
        label_str = (
            ":".join(_escape_identifier(i) for i in sorted(node.labels))
            if node.labels
            else "NoLabel"
        )

        query = f"""
        MERGE (n:{label_str} {{element_id: $element_id}})
        SET n = $props
        RETURN n
        """
        result = tx.run(
            query,
            element_id=node.element_id,
            props=node.to_python_map(
                keep_element_id=True,  # node.element_id도 property에 저장
                element_id_val=node.element_id,
            ),
        ).single()
        return result["n"] if result else {}

    def _do_upsert_relationship(
        self, tx: ManagedTransaction, relationship: Relationship
    ) -> dict:
        """
        Merge relationship based on relationship.element_id in a transaction,
        upsert start_node, end_node first in the same transaction.
        Use temporary label "NoLabel" if no label is present.
        """
        self._do_upsert_node(tx, relationship.start_node)
        self._do_upsert_node(tx, relationship.end_node)

        start_labels = (
            ":".join(
                _escape_identifier(i)
                for i in sorted(relationship.start_node.labels)
            )
            or "NoLabel"
        )
        end_labels = (
            ":".join(
                _escape_identifier(i)
                for i in sorted(relationship.end_node.labels)
            )
            or "NoLabel"
        )
        rel_type = _escape_identifier(relationship.rel_type)

        query = f"""
        MATCH (start:{start_labels} {{element_id: $startId}})
        MATCH (end:{end_labels}     {{element_id: $endId}})

        MERGE (start)-[r:{rel_type} {{element_id: $relId}}]->(end)
        SET r = $props
        RETURN r
        """
        result = tx.run(
            query,
            startId=relationship.start_node.element_id,
            endId=relationship.end_node.element_id,
            relId=relationship.element_id,
            props=relationship.to_python_map(
                keep_element_id=True,
                element_id_val=relationship.element_id,
            ),
        ).single()
        return result["r"] if result else {}

    @with_session.readwrite_transaction
    def upsert_node(self, tx: ManagedTransaction, node: Node) -> dict:
        """Upsert a node in a transaction"""
        return self._do_upsert_node(tx, node)

    @with_session.readwrite_transaction
    def upsert_relationship(
        self, tx: ManagedTransaction, rel: Relationship
    ) -> dict:
        """Upsert a relationship in a transaction"""
        return self._do_upsert_relationship(tx, rel)

    @with_session.readwrite_transaction
    def upsert_graph(self, tx: ManagedTransaction, graph: Graph) -> None:
        """
        Upsert all Node, Relationship in a Graph within a single transaction.
        """
        # 1) 모든 노드 업서트
        for node in graph.nodes.values():
            self._do_upsert_node(tx, node)

        # 2) 모든 관계 업서트
        for rel in graph.relationships.values():
            self._do_upsert_relationship(tx, rel)
