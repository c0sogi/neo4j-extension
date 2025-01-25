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
            query=REL_QUERY, params={"EXCLUDED_LABELS": excluded_labels}
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
            "metadata": {"constraint": constraint_res, "index": index_res},
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
        """
        Merge node based on globalId if present. 라벨로 매칭하지 않고, globalId만 사용.
        라벨은 이후에 'SET n:SomeLabel' 형태로 붙임.
        """
        if node.globalId:
            # globalId만으로 MERGE
            query: LiteralString = f"""
                MERGE (n {{ globalId: $globalId }})
                ON CREATE SET n.createdAt = timestamp()
                ON MATCH  SET n.updatedAt = timestamp()
                SET n += $props
                SET n:{node.label_str}
                RETURN n
            """
            result = tx.run(
                query, globalId=node.globalId, props=node.to_python_props()
            ).single()
        else:
            # globalId 없으면 CREATE
            query = f"""
                CREATE (n:{node.label_str})
                SET n = $props
                RETURN n
            """
            result = tx.run(query, props=node.to_python_props()).single()

        return result["n"] if result else {}

    def _do_upsert_relationship(
        self, tx: ManagedTransaction, relationship: Relationship
    ) -> dict:
        """
        Merge relationship based on relationship.globalId if present, using the relationship type.
        Upsert start_node, end_node first with globalId only.
        """
        # [변경됨!] start_node, end_node도 label 없이 { globalId: ~ }로 매칭
        self._do_upsert_node(tx, relationship.start_node)
        self._do_upsert_node(tx, relationship.end_node)

        rel_type: LiteralString = relationship.rel_type

        if relationship.globalId:
            query: LiteralString = f"""
                MATCH (start {{globalId: $startNodeGlobalId}})
                MATCH (end   {{globalId: $endNodeGlobalId}})
                MERGE (start)-[r:{rel_type} {{ globalId: $relGlobalId }}]->(end)
                ON CREATE SET r.createdAt = timestamp()
                ON MATCH  SET r.updatedAt = timestamp()
                SET r += $props
                RETURN r
            """
            result = tx.run(
                query,
                startNodeGlobalId=relationship.start_node.globalId,
                endNodeGlobalId=relationship.end_node.globalId,
                relGlobalId=relationship.globalId,
                props=relationship.to_python_props(),
            ).single()
        else:
            # globalId 없는 관계면 무조건 CREATE
            query = f"""
                MATCH (start {{globalId: $startNodeGlobalId}})
                MATCH (end   {{globalId: $endNodeGlobalId}})
                CREATE (start)-[r:{rel_type}]->(end)
                SET r = $props
                RETURN r
            """
            result = tx.run(
                query,
                startNodeGlobalId=relationship.start_node.globalId,
                endNodeGlobalId=relationship.end_node.globalId,
                props=relationship.to_python_props(),
            ).single()

        return result["r"] if result else {}

    @with_session.readwrite_transaction
    def clear_all(self, tx: ManagedTransaction) -> None:
        """Clear all data in the database"""
        tx.run("MATCH (n) DETACH DELETE n")

    @with_async_session.readwrite_transaction
    async def aclear_all(self, tx: AsyncManagedTransaction) -> None:
        """Clear all data in the database"""
        await tx.run("MATCH (n) DETACH DELETE n")

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
        for node in graph.nodes.values():
            self._do_upsert_node(tx, node)
        for rel in graph.relationships.values():
            self._do_upsert_relationship(tx, rel)
