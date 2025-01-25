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

from .conversion import ensure_python_type
from .graph import Graph, Node, Relationship
from .typing import GraphSchema, Property
from .utils import escape_identifier

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


class with_session:
    """
    동기용 데코레이터 모음.
    """

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
    """
    비동기용 데코레이터 모음.
    """

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
    """
    Neo4j Connection

    Attributes:
        host: str
        port: str
        password: str
        user: str
        protocol: str
        driver: Optional[Driver]
        async_driver: Optional[AsyncDriver]
        driver_kwargs: DriverKwargs
        async_driver_kwargs: AsyncDriverKwargs
        session_kwargs: SessionKwargs
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

    @with_session.scope
    def get_graph_schema(
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

    def get_formatted_graph_schema(
        self,
        excluded_labels: Optional[list[str]] = None,
        excluded_rels: Optional[list[str]] = None,
    ) -> str:
        return self.format_graph_schema(
            self.get_graph_schema(
                excluded_labels=excluded_labels, excluded_rels=excluded_rels
            )
        )

    @staticmethod
    def format_graph_schema(graph_schema: GraphSchema) -> str:
        lines: list[str] = []
        lines.append("### Node properties")
        node_props: dict[str, list[Property]] = graph_schema.get(
            "node_props", {}
        )
        for label, props in node_props.items():
            lines.append(f"- {label}")
            for p in props:
                lines.append(f"  * {p['property']}: {p['type']}")
        lines.append("\n### Relationship properties")
        rel_props: dict[str, list[Property]] = graph_schema.get(
            "rel_props", {}
        )
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
        Merge node based on globalId if present.
        """
        if node.globalId:
            query: LiteralString = f"""
                MERGE (n {{ globalId: $globalId }})
                SET n += $props
                SET n:{node.safe_labelstring}
                RETURN n
            """
            result = tx.run(
                query, globalId=node.globalId, props=node.to_python_props()
            ).single()
        else:
            query: LiteralString = f"""
                CREATE (n:{node.safe_labelstring})
                SET n = $props
                RETURN n
            """
            result = tx.run(query, props=node.to_python_props()).single()
        return result["n"] if result else {}

    def _do_upsert_relationship(
        self, tx: ManagedTransaction, relationship: Relationship
    ) -> dict:
        """
        Merge relationship based on relationship.globalId if present.
        Upsert start_node, end_node first with globalId only.
        """
        self._do_upsert_node(tx, relationship.start_node)
        self._do_upsert_node(tx, relationship.end_node)
        if relationship.globalId:
            query: LiteralString = f"""
                MATCH (start {{globalId: $startNodeGlobalId}})
                MATCH (end   {{globalId: $endNodeGlobalId}})
                MERGE (start)-[r:{escape_identifier(relationship.rel_type)} {{ globalId: $relGlobalId }}]->(end)
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
            query: LiteralString = f"""
                MATCH (start {{globalId: $startNodeGlobalId}})
                MATCH (end   {{globalId: $endNodeGlobalId}})
                CREATE (start)-[r:{escape_identifier(relationship.rel_type)}]->(end)
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
        """Clear all data in the database (async)"""
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

    # --------------------------------------------------------------------------------
    # (Below) New utility methods for node/relationship search, update, delete, etc.
    # Synchronous versions
    # --------------------------------------------------------------------------------

    @with_session.readonly_transaction
    def find_node_by_global_id(
        self, tx: ManagedTransaction, global_id: str
    ) -> Optional[Node]:
        """
        주어진 global_id를 가진 노드를 조회하여 Node 객체로 반환한다.
        없으면 None.
        """
        query: LiteralString = """
        MATCH (n {globalId: $globalId})
        RETURN n
        """
        record = tx.run(query, globalId=global_id).single()
        if record is None:
            return None
        neo4j_node = record["n"]
        return Node.from_neo4j(neo4j_node)

    @with_session.readonly_transaction
    def match_nodes(
        self,
        tx: ManagedTransaction,
        label: str,
        property_filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> list[Node]:
        """
        특정 레이블과 속성 조건으로 노드 목록을 조회한다.
        property_filters: { 'name': 'Alice', 'age': 20 }
        limit: 결과 개수 제한
        """
        where_clauses: list[LiteralString] = []
        params: dict[str, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"p{idx}")
                where_clauses.append(
                    f"n.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        limit_clause: LiteralString = ""
        if limit is not None:
            limit_clause = cast(LiteralString, f"LIMIT {limit}")

        query_str = f"""
        MATCH (n:{escape_identifier(label)})
        {where_str}
        RETURN n
        {limit_clause}
        """
        result = tx.run(cast(LiteralString, query_str), **params)
        nodes: list[Node] = []
        for record in result:
            neo4j_node = record["n"]
            nodes.append(Node.from_neo4j(neo4j_node))
        return nodes

    @with_session.readonly_transaction
    def match_relationships(
        self,
        tx: ManagedTransaction,
        rel_type: str,
        property_filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> list[Relationship]:
        """
        특정 relationship 타입과 속성 조건을 만족하는 관계 목록을 조회한다.
        """
        where_clauses: list[LiteralString] = []
        params: dict[str, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"p{idx}")
                where_clauses.append(
                    f"r.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        limit_clause: LiteralString = ""
        if limit is not None:
            limit_clause = cast(LiteralString, f"LIMIT {limit}")

        query_str: LiteralString = f"""
        MATCH ()-[r:{escape_identifier(rel_type)}]->()
        {where_str}
        RETURN r
        {limit_clause}
        """
        result = tx.run(cast(LiteralString, query_str), **params)
        rels: list[Relationship] = []
        for record in result:
            neo4j_rel = record["r"]
            rels.append(Relationship.from_neo4j(neo4j_rel))
        return rels

    @with_session.readonly_transaction
    def find_nodes_in_relationship(
        self,
        tx: ManagedTransaction,
        rel_type: str,
        property_filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> list[tuple[Node, Relationship, Node]]:
        """
        주어진 관계 타입(rel_type)과 property_filters를 만족하는
        (start_node, relationship, end_node) 목록을 조회한다.
        """
        where_clauses: list[LiteralString] = []
        params: dict[str, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"p{idx}")
                where_clauses.append(
                    f"r.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        limit_clause: LiteralString = ""
        if limit is not None:
            limit_clause = cast(LiteralString, f"LIMIT {limit}")

        query_str: LiteralString = f"""
        MATCH (start)-[r:{escape_identifier(rel_type)}]->(end)
        {where_str}
        RETURN start, r, end
        {limit_clause}
        """
        result = tx.run(query_str, **params)
        output: list[tuple[Node, Relationship, Node]] = []
        for record in result:
            start_node = Node.from_neo4j(record["start"])
            rel_obj = Relationship.from_neo4j(record["r"])
            end_node = Node.from_neo4j(record["end"])
            output.append((start_node, rel_obj, end_node))
        return output

    @with_session.readwrite_transaction
    def delete_node_by_global_id(
        self, tx: ManagedTransaction, global_id: str
    ) -> None:
        """
        global_id를 가진 노드를 (관계까지) 삭제한다.
        """
        query: LiteralString = """
        MATCH (n {globalId: $gid})
        DETACH DELETE n
        """
        tx.run(query, gid=global_id)

    @with_session.readwrite_transaction
    def delete_nodes_by_label(
        self,
        tx: ManagedTransaction,
        label: str,
        property_filters: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        특정 레이블과 속성 조건을 만족하는 노드들을 일괄 삭제한다.
        (관계까지 포함)
        """
        where_clauses: list[LiteralString] = []
        params: dict[str, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"pf{idx}")
                where_clauses.append(
                    f"n.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        query_str: LiteralString = f"""
        MATCH (n:{escape_identifier(label)})
        {where_str}
        DETACH DELETE n
        """
        tx.run(query_str, **params)

    @with_session.readwrite_transaction
    def delete_relationship_by_global_id(
        self, tx: ManagedTransaction, global_id: str
    ) -> None:
        """
        globalId를 가진 관계를 찾아 삭제한다.
        """
        query: LiteralString = """
        MATCH ()-[r {globalId: $gid}]-()
        DELETE r
        """
        tx.run(query, gid=global_id)

    @with_session.readwrite_transaction
    def delete_relationships_by_type(
        self,
        tx: ManagedTransaction,
        rel_type: str,
        property_filters: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        특정 관계 타입과 속성 조건을 만족하는 관계들을 일괄 삭제한다.
        """
        where_clauses: list[LiteralString] = []
        params: dict[str, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"pf{idx}")
                where_clauses.append(
                    f"r.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        query_str: LiteralString = f"""
        MATCH ()-[r:{escape_identifier(rel_type)}]->()
        {where_str}
        DELETE r
        """
        tx.run(query_str, **params)

    @with_session.readwrite_transaction
    def update_node_properties(
        self,
        tx: ManagedTransaction,
        global_id: str,
        new_properties: dict[str, Any],
    ) -> None:
        """
        global_id 노드의 속성을 업데이트한다.
        존재하지 않는 속성은 새로 추가, 기존에 있으면 덮어쓴다.
        """
        query: LiteralString = """
        MATCH (n {globalId: $gid})
        SET n += $props
        """
        tx.run(query, gid=global_id, props=new_properties)

    @with_session.readwrite_transaction
    def remove_node_property(
        self, tx: ManagedTransaction, global_id: str, property_key: str
    ) -> None:
        """
        global_id 노드에서 특정 property 하나를 제거한다.
        """
        query: LiteralString = f"""
        MATCH (n {{globalId: $gid}})
        SET n.{escape_identifier(property_key)} = null
        """
        tx.run(query, gid=global_id)

    @with_session.readwrite_transaction
    def add_labels_to_node(
        self, tx: ManagedTransaction, global_id: str, labels: list[str]
    ) -> None:
        """
        global_id 노드에 새로운 레이블들을 추가한다.
        예: SET n:LabelA:LabelB
        """
        if not labels:
            return
        label_clause: LiteralString = cast(
            LiteralString,
            "SET n"
            + "".join(
                f":{lbl}" for lbl in (escape_identifier(lb) for lb in labels)
            ),
        )
        query: LiteralString = f"""
        MATCH (n {{globalId: $gid}})
        {label_clause}
        """
        tx.run(query, gid=global_id)

    @with_session.readwrite_transaction
    def remove_labels_from_node(
        self, tx: ManagedTransaction, global_id: str, labels: list[str]
    ) -> None:
        """
        global_id 노드에서 특정 레이블들을 제거한다.
        예: REMOVE n:LabelA:LabelB
        """
        if not labels:
            return
        remove_clause: LiteralString = cast(
            LiteralString,
            "REMOVE n"
            + "".join(
                f":{lbl}" for lbl in (escape_identifier(lb) for lb in labels)
            ),
        )
        query: LiteralString = f"""
        MATCH (n {{globalId: $gid}})
        {remove_clause}
        """
        tx.run(query, gid=global_id)

    @with_session.readwrite_transaction
    def update_relationship_properties(
        self,
        tx: ManagedTransaction,
        global_id: str,
        new_properties: dict[str, Any],
    ) -> None:
        """
        global_id 관계의 속성을 업데이트한다.
        """
        query: LiteralString = """
        MATCH ()-[r {globalId: $gid}]->()
        SET r += $props
        """
        tx.run(query, gid=global_id, props=new_properties)

    @with_session.readwrite_transaction
    def link_nodes(
        self,
        tx: ManagedTransaction,
        start_node_global_id: str,
        end_node_global_id: str,
        rel_type: str,
        properties: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        start_node_global_id와 end_node_global_id를 가진 노드를
        주어진 관계(rel_type)로 연결한다. 없으면 생성, 있으면 업데이트.
        """

        query: LiteralString = f"""
        MATCH (start {{globalId: $startGid}})
        MATCH (end {{globalId: $endGid}})
        MERGE (start)-[r:{escape_identifier(rel_type)}]->(end)
        SET r += $props
        """
        tx.run(
            query,
            startGid=start_node_global_id,
            endGid=end_node_global_id,
            props=properties or {},
        )

    @with_session.readonly_transaction
    def get_node_properties(
        self, tx: ManagedTransaction, global_id: str
    ) -> Optional[dict[str, Any]]:
        """
        global_id 노드의 모든 속성을 Python dict 형태로 반환한다.
        """
        query: LiteralString = """
        MATCH (n {globalId: $gid})
        RETURN n
        """
        rec = tx.run(query, gid=global_id).single()
        if rec is None:
            return None
        n = rec["n"]
        node_obj = Node.from_neo4j(n)
        return {
            k: ensure_python_type(v) for k, v in node_obj.properties.items()
        }

    @with_session.readonly_transaction
    def get_relationship_properties(
        self, tx: ManagedTransaction, global_id: str
    ) -> Optional[dict[str, Any]]:
        """
        global_id 관계의 모든 속성을 Python dict 형태로 반환한다.
        """
        query: LiteralString = """
        MATCH ()-[r {globalId: $gid}]->()
        RETURN r
        """
        rec = tx.run(query, gid=global_id).single()
        if rec is None:
            return None
        r = rec["r"]
        rel_obj = Relationship.from_neo4j(r)
        return {
            k: ensure_python_type(v) for k, v in rel_obj.properties.items()
        }

    @with_session.readonly_transaction
    def count_nodes(self, tx: ManagedTransaction, label: str) -> int:
        """
        특정 레이블을 가진 노드의 총 개수를 반환한다.
        """
        query: LiteralString = f"""
        MATCH (n:{escape_identifier(label)})
        RETURN count(n) as cnt
        """
        record = tx.run(cast(LiteralString, query)).single()
        return record["cnt"] if record else 0

    @with_session.readonly_transaction
    def count_relationships(
        self, tx: ManagedTransaction, rel_type: str
    ) -> int:
        """
        특정 관계 타입을 가진 관계의 총 개수를 반환한다.
        """
        query: LiteralString = f"""
        MATCH ()-[r:{escape_identifier(rel_type)}]->()
        RETURN count(r) as cnt
        """
        record = tx.run(cast(LiteralString, query)).single()
        return record["cnt"] if record else 0

    # --------------------------------------------------------------------------------
    # (Below) Async versions of the same utilities (using with_async_session)
    # --------------------------------------------------------------------------------

    @with_async_session.readonly_transaction
    async def afind_node_by_global_id(
        self, tx: AsyncManagedTransaction, global_id: str
    ) -> Optional[Node]:
        """
        주어진 global_id를 가진 노드를 조회 (비동기)
        """
        query: LiteralString = """
        MATCH (n {globalId: $globalId})
        RETURN n
        """
        result = await tx.run(query, globalId=global_id)
        record = await result.single()
        if record is None:
            return None
        return Node.from_neo4j(record["n"])

    @with_async_session.readonly_transaction
    async def amatch_nodes(
        self,
        tx: AsyncManagedTransaction,
        label: str,
        property_filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> list[Node]:
        """
        특정 레이블과 속성 조건으로 노드 목록을 조회 (비동기)
        """
        where_clauses: list[LiteralString] = []
        params: dict[LiteralString, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"p{idx}")
                where_clauses.append(
                    f"n.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        limit_clause: LiteralString = ""
        if limit is not None:
            limit_clause = cast(LiteralString, f"LIMIT {limit}")

        query_str: LiteralString = f"""
        MATCH (n:{escape_identifier(label)})
        {where_str}
        RETURN n
        {limit_clause}
        """
        result = await tx.run(query_str, **params)
        records: list[neo4j.Record] = []
        async for rec in result:
            records.append(rec)
        nodes: list[Node] = []
        for record in records:
            neo4j_node = record["n"]
            nodes.append(Node.from_neo4j(neo4j_node))
        return nodes

    @with_async_session.readonly_transaction
    async def amatch_relationships(
        self,
        tx: AsyncManagedTransaction,
        rel_type: str,
        property_filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> list[Relationship]:
        """
        특정 relationship 타입과 속성 조건을 만족하는 관계 목록을 조회 (비동기)
        """
        where_clauses: list[LiteralString] = []
        params: dict[str, Any] = {}
        if property_filters:
            for idx, (k, v) in enumerate(property_filters.items()):
                param_key: LiteralString = cast(LiteralString, f"p{idx}")
                where_clauses.append(
                    f"r.{escape_identifier(k)} = ${param_key}"
                )
                params[param_key] = v

        where_str: LiteralString = ""
        if where_clauses:
            where_str = "WHERE " + " AND ".join(where_clauses)

        limit_clause: LiteralString = ""
        if limit is not None:
            limit_clause = cast(LiteralString, f"LIMIT {limit}")

        query_str: LiteralString = f"""
        MATCH ()-[r:{escape_identifier(rel_type)}]->()
        {where_str}
        RETURN r
        {limit_clause}
        """
        result = await tx.run(cast(LiteralString, query_str), **params)
        records: list[neo4j.Record] = []
        async for rec in result:
            records.append(rec)
        rels: list[Relationship] = []
        for record in records:
            rels.append(Relationship.from_neo4j(record["r"]))
        return rels

    @with_async_session.readwrite_transaction
    async def adelete_node_by_global_id(
        self, tx: AsyncManagedTransaction, global_id: str
    ) -> None:
        """
        global_id를 가진 노드를 (관계까지) 삭제 (비동기)
        """
        query: LiteralString = """
        MATCH (n {globalId: $gid})
        DETACH DELETE n
        """
        await tx.run(query, gid=global_id)

    @with_async_session.readwrite_transaction
    async def adelete_relationship_by_global_id(
        self, tx: AsyncManagedTransaction, global_id: str
    ) -> None:
        """
        globalId를 가진 관계를 찾아 삭제 (비동기)
        """
        query: LiteralString = """
        MATCH ()-[r {globalId: $gid}]-()
        DELETE r
        """
        await tx.run(query, gid=global_id)

    @with_async_session.readwrite_transaction
    async def aupdate_node_properties(
        self,
        tx: AsyncManagedTransaction,
        global_id: str,
        new_properties: dict[str, Any],
    ) -> None:
        """
        global_id 노드의 속성을 업데이트한다. (비동기)
        """
        query: LiteralString = """
        MATCH (n {globalId: $gid})
        SET n += $props
        """
        await tx.run(query, gid=global_id, props=new_properties)

    @with_async_session.readwrite_transaction
    async def aupdate_relationship_properties(
        self,
        tx: AsyncManagedTransaction,
        global_id: str,
        new_properties: dict[str, Any],
    ) -> None:
        """
        global_id 관계의 속성을 업데이트 (비동기)
        """
        query: LiteralString = """
        MATCH ()-[r {globalId: $gid}]->()
        SET r += $props
        """
        await tx.run(query, gid=global_id, props=new_properties)

    @with_async_session.readwrite_transaction
    async def alink_nodes(
        self,
        tx: AsyncManagedTransaction,
        start_node_global_id: str,
        end_node_global_id: str,
        rel_type: str,
        properties: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        비동기 버전: 두 노드를 rel_type으로 연결한다. 없으면 생성, 있으면 업데이트.
        """
        query: LiteralString = f"""
        MATCH (start {{globalId: $startGid}})
        MATCH (end {{globalId: $endGid}})
        MERGE (start)-[r:{escape_identifier(rel_type)}]->(end)
        SET r += $props
        """
        await tx.run(
            query,
            startGid=start_node_global_id,
            endGid=end_node_global_id,
            props=properties or {},
        )

    @with_async_session.readonly_transaction
    async def acount_nodes(
        self, tx: AsyncManagedTransaction, label: str
    ) -> int:
        """
        특정 레이블을 가진 노드 총 개수 (비동기)
        """
        query: LiteralString = f"""
        MATCH (n:{escape_identifier(label)})
        RETURN count(n) as cnt
        """
        result = await tx.run(query)
        record = await result.single()
        if record is None:
            return 0
        return record["cnt"]

    @with_async_session.readonly_transaction
    async def acount_relationships(
        self, tx: AsyncManagedTransaction, rel_type: str
    ) -> int:
        """
        특정 관계 타입을 가진 관계 총 개수 (비동기)
        """
        query: LiteralString = f"""
        MATCH ()-[r:{escape_identifier(rel_type)}]->()
        RETURN count(r) as cnt
        """
        result = await tx.run(query)
        record = await result.single()
        if record is None:
            return 0
        return record["cnt"]
