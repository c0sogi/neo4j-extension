import math
import unittest
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional

from neo4j import ManagedTransaction, Result
from neo4j.exceptions import ServiceUnavailable

from neo4j_extension import (
    Graph,
    GraphSchema,
    Neo4jBoolean,
    Neo4jByteArray,
    Neo4jConnection,
    Neo4jDate,
    Neo4jDuration,
    Neo4jFloat,
    Neo4jInteger,
    Neo4jList,
    Neo4jLocalDateTime,
    Neo4jLocalTime,
    Neo4jMap,
    Neo4jNull,
    Neo4jPoint,
    Neo4jString,
    Neo4jZonedDateTime,
    Node,
    PointValue,
    Relationship,
    convert_neo4j_to_python,
    ensure_neo4j_type,
    get_safe_query,
    split_by_comma_top_level,
    tokenize_cypher_expression,
    with_session,
)


class Neo4jTestconnection(Neo4jConnection):
    """
    기존 test.py에서 활용되는 Connection 상속.
    TEST 레이블 위주로만 데이터를 cleanup하는 메서드 추가.
    """

    @with_session.readwrite_transaction
    def clear_all_test_data(self, tx: ManagedTransaction) -> Result:
        """테스트 데이터만 정리 (TEST 레이블이 있는 노드만)"""
        return tx.run("MATCH (n:TEST) DETACH DELETE n")

    @with_session.readonly_transaction
    def get_all_test_data(
        self, tx: ManagedTransaction
    ) -> list[dict[str, Any]]:
        result: Result = tx.run("MATCH (n:TEST) RETURN n")
        return [dict(record["n"]) for record in result]

    @with_session.readwrite_transaction
    def create_person(
        self, tx: ManagedTransaction, name: str, age: int
    ) -> Result:
        return tx.run(
            "CREATE (p:Person:TEST {name: $name, age: $age})",
            name=name,
            age=age,
        )

    @with_session.readonly_transaction
    def get_all_person_age(
        self, tx: ManagedTransaction
    ) -> list[tuple[str, int]]:
        result: Result = tx.run(
            "MATCH (p:Person:TEST) RETURN p.name AS name, p.age AS age"
        )
        return [(record["name"], record["age"]) for record in result]

    @with_session.readwrite_transaction
    def create_relationship(
        self,
        tx: ManagedTransaction,
        from_global_id: str,
        to_global_id: str,
        rel_type: str,
    ) -> Result:
        query = get_safe_query(
            "MATCH (from:TEST {{globalId: $from_global_id}}) "
            "MATCH (to:TEST {{globalId: $to_global_id}}) "
            "CREATE (from)-[:{rel_type}]->(to)",
            rel_type=rel_type,
        )
        return tx.run(
            query, from_global_id=from_global_id, to_global_id=to_global_id
        )

    @with_session.readwrite_transaction
    def delete_relationship(
        self,
        tx: ManagedTransaction,
        from_global_id: str,
        to_global_id: str,
        rel_type: str,
    ) -> Result:
        query = get_safe_query(
            "MATCH (from:TEST {{globalId: $from_global_id}})-[r:{rel_type}]->(to:TEST {{globalId: $to_global_id}}) "
            "DELETE r",
            rel_type=rel_type,
        )
        return tx.run(
            query, from_global_id=from_global_id, to_global_id=to_global_id
        )


class TestPersonOperations(unittest.TestCase):
    """
    기존의 사람(Person) 노드 생성 및 조회에 대한 테스트.
    추가로 업데이트, 제거 등에 대한 시나리오도 확장.
    """

    @classmethod
    def setUpClass(cls):
        cls.conn = Neo4jTestconnection()
        try:
            cls.conn.clear_all_test_data()
        except ServiceUnavailable:
            raise RuntimeError(
                "Neo4j DB에 연결할 수 없습니다. 테스트 환경을 확인해주세요."
            )

    @classmethod
    def tearDownClass(cls):
        if cls.conn:
            cls.conn.clear_all_test_data()
            cls.conn.close()

    def setUp(self):
        self.conn.clear_all_test_data()
        self.conn.close()

    def tearDown(self):
        self.conn.clear_all_test_data()
        self.conn.close()

    def test_create_person(self):
        """사람 생성 및 조회 테스트"""
        # Given
        self.conn.create_person("John Doe", 30)

        # When
        persons: list[tuple[str, int]] = self.conn.get_all_person_age()

        # Then
        self.assertIsNotNone(persons, "persons should not be None")
        self.assertEqual(len(persons), 1, "should have exactly one person")
        self.assertIn(("John Doe", 30), persons)

    def test_get_all_persons(self):
        """여러 명 생성 및 조회"""
        # Given
        test_data = [
            ("John Doe", 30),
            ("Jane Kim", 25),
        ]
        for name, age in test_data:
            self.conn.create_person(name, age)

        # When
        persons = self.conn.get_all_person_age()

        # Then
        self.assertIsNotNone(persons, "persons should not be None")
        self.assertEqual(len(persons), 2, "should have exactly two persons")
        for person in test_data:
            self.assertIn(person, persons)

    def test_update_person(self):
        """업데이트 시나리오 테스트"""
        # Given
        self.conn.create_person("Alice", 20)
        self.conn.create_person("Bob", 25)
        persons_before = self.conn.get_all_person_age()
        self.assertEqual(len(persons_before), 2)

        # When - Bob의 age를 30으로 변경하는 로직을 직접 Cypher로 실행
        # (간단히, Person:TEST label이므로 식별)
        @with_session.readwrite_transaction
        def update_bob_age(
            conn: Neo4jTestconnection, tx: ManagedTransaction
        ):
            return tx.run(
                "MATCH (p:Person:TEST {name: 'Bob'}) SET p.age = 30 RETURN p.age"
            )

        update_bob_age(self.conn)
        persons_after = self.conn.get_all_person_age()

        # Then
        self.assertIn(("Bob", 30), persons_after)
        self.assertNotIn(("Bob", 25), persons_after)

    def test_delete_person(self):
        """노드 삭제 테스트"""
        # Given
        self.conn.create_person("Charlie", 40)
        self.assertEqual(len(self.conn.get_all_person_age()), 1)

        # When - Charlie 노드 직접 삭제
        @with_session.readwrite_transaction
        def delete_charlie(
            conn: Neo4jTestconnection, tx: ManagedTransaction
        ):
            return tx.run(
                "MATCH (p:Person:TEST {name:'Charlie'}) DETACH DELETE p"
            )

        delete_charlie(self.conn)

        # Then
        self.assertEqual(len(self.conn.get_all_person_age()), 0)


class TestConversionAndTypes(unittest.TestCase):
    """
    conversion.py, primitive.py, temporal.py, spatial.py 등
    주요 타입의 to_cypher, from_cypher, 변환 함수 등을 체계적으로 테스트.
    """

    def test_basic_types_roundtrip(self):
        """문자열, 숫자, bool, null 등 간단한 타입 변환 테스트"""
        data_pairs = [
            (None, Neo4jNull()),
            (True, Neo4jBoolean(True)),
            (False, Neo4jBoolean(False)),
            (42, Neo4jInteger(42)),
            (-999999999999, Neo4jInteger(-999999999999)),
            (3.14, Neo4jFloat(3.14)),
            ("Hello", Neo4jString("Hello")),
            (b"binary\x00data", Neo4jByteArray(b"binary\x00data")),
        ]
        for py_val, neo4j_type in data_pairs:
            with self.subTest(py_val=py_val, neo4j_type=neo4j_type):
                # Python -> Neo4jType
                converted = ensure_neo4j_type(py_val)
                self.assertEqual(converted, neo4j_type)

                # to_cypher -> from_cypher -> Python
                cypher_expr = converted.to_cypher()
                parsed_obj = converted.from_cypher(cypher_expr)
                self.assertEqual(parsed_obj, converted)

                # 다시 Python 기반 값과 같은지
                roundtrip_py_val = convert_neo4j_to_python(parsed_obj)
                # int 범위 넘어가면 OverflowError 발생 가능
                self.assertEqual(roundtrip_py_val, py_val)

    def test_float_special_values(self):
        """NaN, Infinity, -Infinity 등 특수 float 처리"""
        special_floats = [float("inf"), float("-inf"), float("nan")]
        for val in special_floats:
            with self.subTest(val=val):
                t = Neo4jFloat(val)
                cypher_str = t.to_cypher()
                parsed = Neo4jFloat.from_cypher(cypher_str)
                if math.isnan(val):
                    self.assertTrue(math.isnan(parsed.value))
                else:
                    self.assertEqual(parsed.value, val)

    def test_date_time_durations(self):
        """날짜, 시간, 날짜/시간, Duration 등 테스트"""
        d = date(2021, 12, 25)
        d_type = Neo4jDate(d)
        d_cypher = d_type.to_cypher()
        self.assertIn("date(", d_cypher)
        self.assertEqual(Neo4jDate.from_cypher(d_cypher).value, d)

        t_local = time(12, 34, 56)
        t_type = Neo4jLocalTime(t_local)
        t_cypher = t_type.to_cypher()
        self.assertIn("time(", t_cypher)
        self.assertEqual(Neo4jLocalTime.from_cypher(t_cypher).value, t_local)

        # LocalDateTime
        dt_local = datetime(2022, 1, 1, 13, 45, 59)
        dt_type = Neo4jLocalDateTime(dt_local)
        dt_cypher = dt_type.to_cypher()
        self.assertIn("datetime(", dt_cypher)
        self.assertEqual(
            Neo4jLocalDateTime.from_cypher(dt_cypher).value, dt_local
        )

        # ZonedDateTime
        dt_zoned = datetime(2022, 3, 14, 10, 0, 0, tzinfo=timezone.utc)
        zdt_type = Neo4jZonedDateTime(dt_zoned)
        zdt_cypher = zdt_type.to_cypher()
        self.assertIn("datetime(", zdt_cypher)
        parsed_zdt = Neo4jZonedDateTime.from_cypher(zdt_cypher).value
        self.assertEqual(parsed_zdt, dt_zoned)

        # Duration
        dur = timedelta(
            days=3, hours=4, minutes=5, seconds=6, microseconds=123456
        )
        dur_type = Neo4jDuration(dur)
        dur_cypher = dur_type.to_cypher()
        parsed_dur = Neo4jDuration.from_cypher(dur_cypher)
        # float 미세차이 등이 있을 수 있으니, total_seconds로 비교
        self.assertAlmostEqual(
            parsed_dur.value.total_seconds(), dur.total_seconds(), places=3
        )

    def test_points(self):
        """Neo4j Point 타입 테스트"""
        p = PointValue(crs="cartesian", x=1.234, y=5.678, z=None)
        np = Neo4jPoint(p)
        c_str = np.to_cypher()
        parsed = Neo4jPoint.from_cypher(c_str)
        self.assertEqual(parsed.value, p)

        # 3D 좌표
        p3d = PointValue(crs="cartesian-3d", x=3.0, y=4.0, z=5.0)
        np3d = Neo4jPoint(p3d)
        c_str3d = np3d.to_cypher()
        parsed3d = Neo4jPoint.from_cypher(c_str3d)
        self.assertEqual(parsed3d.value, p3d)

        # 잘못된 literal
        with self.assertRaises(ValueError):
            Neo4jPoint.from_cypher("point({x: 10, yyy: 20})")

    def test_maps_and_lists(self):
        """Neo4jMap, Neo4jList를 통한 복합 구조 변환 테스트"""
        # Map
        m = Neo4jMap(
            {
                "key": Neo4jString("value"),
                "num": Neo4jInteger(123),
            }
        )
        c_str = m.to_cypher()
        parsed = Neo4jMap.from_cypher(c_str)
        self.assertEqual(parsed.value["key"].value, "value")
        self.assertEqual(parsed.value["num"].value, 123)

        # List
        lst = Neo4jList(
            [
                Neo4jString("abc"),
                Neo4jInteger(999),
                Neo4jNull(),
            ]
        )
        c_str = lst.to_cypher()
        parsed = Neo4jList.from_cypher(c_str)
        self.assertEqual(len(parsed.value), 3)
        self.assertIsInstance(parsed.value[0], Neo4jString)
        self.assertIsInstance(parsed.value[1], Neo4jInteger)
        self.assertIsInstance(parsed.value[2], Neo4jNull)


class TestGraphEntities(unittest.TestCase):
    """
    graph.py 내 Node, Relationship, Graph 사용성 테스트.
    - 속성, label, globalId 유무
    - to_cypher / from_neo4j, is_storable_as_property 등 점검
    """

    def setUp(self):
        self.conn = Neo4jTestconnection()
        self.conn.clear_all_test_data()

    def tearDown(self):
        self.conn.clear_all_test_data()
        self.conn.close()

    def test_create_and_upsert_node(self):
        node = Node(
            properties={"name": "Keanu Reeves", "age": 42},
            labels={"Person", "Actor", "TEST"},
            globalId="test_node",
        )
        self.conn.upsert_node(node)
        data = self.conn.get_all_test_data()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["name"], "Keanu Reeves")

        # 글로벌 ID가 있으면 하나만 유지
        node["age"] = 50
        self.conn.upsert_node(node)
        data = self.conn.get_all_test_data()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["age"], 50)

    def test_create_relationship_basic(self):
        node1 = Node({"name": "A"}, {"TEST"}, "nodeA")
        node2 = Node({"name": "B"}, {"TEST"}, "nodeB")
        self.conn.upsert_node(node1)
        self.conn.upsert_node(node2)

        self.conn.create_relationship("nodeA", "nodeB", "LIKES")

        # Cypher로 관계가 제대로 만들어졌는지 확인
        @with_session.readonly_transaction
        def check_rel(conn: Neo4jTestconnection, tx: ManagedTransaction):
            r = tx.run(
                "MATCH (a:TEST {globalId:'nodeA'})-[rel:LIKES]->(b:TEST {globalId:'nodeB'}) RETURN rel"
            )
            return r.single()

        rel_record = check_rel(self.conn)
        self.assertIsNotNone(
            rel_record,
            "Relationship LIKES should exist between nodeA and nodeB",
        )

    def test_upsert_relationship(self):
        node1 = Node({"name": "A"}, {"TEST"}, "nodeA")
        node2 = Node({"name": "B"}, {"TEST"}, "nodeB")
        rel = Relationship(
            properties={"since": 2020},
            rel_type="FRIEND",
            start_node=node1,
            end_node=node2,
            globalId="a_friend_b",
        )
        self.conn.upsert_relationship(rel)

        # 다시 upsert
        rel["since"] = 2021
        self.conn.upsert_relationship(rel)

        @with_session.readonly_transaction
        def check_since(
            conn: Neo4jTestconnection, tx: ManagedTransaction
        ) -> Optional[int]:
            res = tx.run(
                "MATCH (a:TEST {globalId:'nodeA'})-[r:FRIEND]->(b:TEST {globalId:'nodeB'}) RETURN r.since AS s"
            )
            rec = res.single()
            return rec["s"] if rec else None

        since_val = check_since(self.conn)
        self.assertEqual(since_val, 2021)

    def test_heterogenous_list_property(self):
        """Node나 Relationship에 int+str 혼합 List 넣을 경우 에러 발생 여부"""
        bad_node = Node(
            globalId="bad_node",
            labels={"TEST"},
            properties={"mixed_list": [1, "two"]},
        )
        with self.assertRaises(ValueError):
            bad_node.to_cypher_props()

    def test_graph_model_in_memory(self):
        """Graph 객체 자체에서 Node/Relationship 추가/삭제 테스트"""
        g = Graph()
        n1 = Node({"prop1": "val1"}, labels={"TEST"}, globalId="n1")
        n2 = Node({"prop2": "val2"}, labels={"TEST"}, globalId="n2")
        g.add_node(n1)
        g.add_node(n2)

        rel = Relationship(
            {"rprop": True}, "RELATED_TO", n1, n2, globalId="rel1"
        )
        g.add_relationship(rel)

        self.assertEqual(len(g.nodes), 2)
        self.assertEqual(len(g.relationships), 1)

        # Node 제거 시 관계도 자동 제거되는지 확인
        g.remove_node("n1")
        self.assertEqual(len(g.nodes), 1)
        self.assertEqual(len(g.relationships), 0)


class TestConnectionAndSession(unittest.TestCase):
    """
    Neo4jConnection 자체 기능(예: connect/close, graph_schema, clear_all, 등) 테스트.
    """

    @classmethod
    def setUpClass(cls):
        cls.conn = Neo4jTestconnection()

    @classmethod
    def tearDownClass(cls):
        if cls.conn:
            cls.conn.close()

    def setUp(self):
        self.conn.clear_all_test_data()
        self.conn.close()

    def tearDown(self):
        self.conn.clear_all_test_data()
        self.conn.close()

    def test_clear_all(self):
        """clear_all 메서드가 DB를 비우는지 확인"""

        # 임의 노드 하나 생성
        @with_session.readwrite_transaction
        def create_test_node(
            conn: Neo4jTestconnection, tx: ManagedTransaction
        ):
            tx.run("CREATE (n:TEST {data: 'X'})")

        create_test_node(self.conn)
        data_before = self.conn.get_all_test_data()
        self.assertEqual(len(data_before), 1)

        # clear_all
        self.conn.clear_all()
        data_after = self.conn.get_all_test_data()
        self.assertEqual(len(data_after), 0)

    def test_graph_schema(self):
        """
        graph_schema, formatted_graph_schema 테스트.
        실제로 APOC이 설치되어 있어야 하며,
        DB에 특정 인덱스/제약조건이 없으면 빈 리스트 등으로 나올 수 있음.
        """
        schema: GraphSchema = self.conn.get_graph_schema()
        # 기본적으로 아무 것도 없는 경우 빈 구조가 반환될 수 있음
        self.assertIn("node_props", schema)
        self.assertIn("rel_props", schema)
        self.assertIn("relationships", schema)
        self.assertIn("metadata", schema)
        self.assertIn("constraint", schema["metadata"])
        self.assertIn("index", schema["metadata"])

        formatted: str = self.conn.get_formatted_graph_schema()
        self.assertIsInstance(formatted, str)
        # 간단히 문자열 길이만이라도 체크
        self.assertGreater(len(formatted), 0)

    def test_connectivity(self):
        """Driver가 연결 가능한지, verify_connectivity 통과하는지 테스트"""
        # 이미 setUpClass에서 connect했지만, 추가 호출 시 문제 없는지 확인
        driver = self.conn.connect()
        self.assertIsNotNone(driver)
        self.assertTrue(
            driver.verify_connectivity() is None
        )  # 성공하면 None 반환

    def test_exceptions(self):
        """의도적으로 잘못된 Cypher 쿼리를 실행하여 예외 발생 테스트"""

        @with_session.readwrite_transaction
        def bad_query(conn: Neo4jTestconnection, tx: ManagedTransaction):
            return tx.run("MATCH (n) CRETE (x) RETURN x")  # 'CREATE' 오타

        with self.assertRaises(Exception):
            bad_query(self.conn)


class TestParsingHelpers(unittest.TestCase):
    """
    utils.py 내 tokenize_cypher_expression, split_by_comma_top_level 기능 테스트
    """

    def test_tokenize_cypher_expression(self):
        expr = "point({ x: 1.23, y: 4.56, crs:'cartesian'})"
        tokens = tokenize_cypher_expression(expr)
        # 대략 괄호, 콜론, 쉼표, 문자열 등을 분리
        self.assertIn("point", tokens)
        self.assertIn("(", tokens)
        self.assertIn(")", tokens)
        self.assertIn("'cartesian'", tokens)

    def test_split_by_comma_top_level(self):
        token_str = "[1, 2, [3, 4], 5]"
        # 하나의 덩어리로 분리되어야 함
        splitted = split_by_comma_top_level([c for c in token_str])
        self.assertEqual(len(splitted), 1)
        self.assertEqual(splitted[0], token_str)

        # 괄호짝 안에서만 쉼표 분리가 되는지 확인
        expr = "1, 2, (3, 4), 5"
        tokens2 = tokenize_cypher_expression(expr)
        splitted2 = split_by_comma_top_level(tokens2)
        self.assertEqual(len(splitted2), 4)
        # (3, 4)는 하나의 덩어리가 됨
        self.assertIn("(3,4)", splitted2[2])


class TestNeo4jConnectionUtilities(unittest.TestCase):
    """
    Neo4jConnection에 정의된 다양한 유틸리티 메서드를 테스트한다.
    (동기 방식 위주)
    """

    @classmethod
    def setUpClass(cls):
        cls.conn = Neo4jTestconnection()
        # 테스트용 라벨 (테스트 중에만 사용)
        cls.test_label = "TESTUTIL"

    @classmethod
    def tearDownClass(cls):
        if cls.conn:
            cls.conn.clear_all()  # 모든 데이터 삭제
            cls.conn.close()

    def setUp(self):
        # 각 테스트 시작 전에 DB 초기화
        self.conn.clear_all_test_data()  # TEST 라벨만 삭제
        self.conn.clear_all()  # 혹시 남은 데이터도 모두 삭제
        # 재연결(테스트 격리)
        self.conn.close()

    def tearDown(self):
        self.conn.clear_all()
        self.conn.close()

    def test_find_and_delete_node_by_global_id(self):
        """
        upsert_node -> find_node_by_global_id -> delete_node_by_global_id
        순으로 노드를 생성/조회/삭제하고 동작 확인
        """
        # 1) 노드를 생성(upsert)
        node = Node(
            properties={"name": "Utility Test", "count": 1},
            labels={self.test_label},
            globalId="util_node_1",
        )
        self.conn.upsert_node(node)

        # 2) find_node_by_global_id로 조회
        found_node = self.conn.find_node_by_global_id("util_node_1")
        self.assertIsNotNone(found_node)
        assert found_node is not None
        self.assertEqual(found_node["name"], "Utility Test")
        self.assertEqual(found_node["count"], 1)

        # 3) 노드 삭제
        self.conn.delete_node_by_global_id("util_node_1")
        after_delete = self.conn.find_node_by_global_id("util_node_1")
        self.assertIsNone(
            after_delete, "노드가 삭제된 뒤에는 조회가 없어야 함"
        )

    def test_match_nodes_and_delete_nodes_by_label(self):
        """
        match_nodes -> 특정 label + 속성조건으로 노드를 찾고,
        delete_nodes_by_label -> 일괄 삭제 확인
        """
        # 노드 2개 생성
        node1 = Node(
            {"name": "Alice", "category": "hero"},
            {self.test_label},
            "alice_id",
        )
        node2 = Node(
            {"name": "Bob", "category": "villain"},
            {self.test_label},
            "bob_id",
        )
        self.conn.upsert_node(node1)
        self.conn.upsert_node(node2)

        # label = TESTUTIL, property_filters = {"category": "hero"}
        matched = self.conn.match_nodes(
            label=self.test_label, property_filters={"category": "hero"}
        )
        self.assertEqual(len(matched), 1, "hero 카테고리는 1개만 있어야 함")
        self.assertEqual(matched[0]["name"], "Alice")

        # delete_nodes_by_label로 모든 TESTUTIL 노드 삭제
        self.conn.delete_nodes_by_label(self.test_label)
        # 모두 삭제되었는지 확인
        matched_after = self.conn.match_nodes(label=self.test_label)
        self.assertEqual(
            len(matched_after), 0, "TESTUTIL 노드가 모두 삭제되어야 함"
        )

    def test_update_node_properties_and_remove_property(self):
        """
        update_node_properties -> 노드 속성 업데이트
        remove_node_property -> 특정 속성만 제거
        """
        node = Node({"name": "Charlie"}, {self.test_label}, "charlie_id")
        self.conn.upsert_node(node)

        # 업데이트 전 상태 확인
        props_before = self.conn.get_node_properties("charlie_id")
        self.assertIsNotNone(props_before)
        assert props_before is not None
        self.assertNotIn("age", props_before, "초기엔 age 속성이 없어야 함")

        # update_node_properties
        self.conn.update_node_properties(
            "charlie_id", {"age": 35, "location": "Earth"}
        )
        props_mid = self.conn.get_node_properties("charlie_id")
        self.assertIsNotNone(props_mid)
        assert props_mid is not None

        self.assertEqual(props_mid["age"], 35)
        self.assertEqual(props_mid["location"], "Earth")

        # remove_node_property
        self.conn.remove_node_property("charlie_id", "location")
        props_after = self.conn.get_node_properties("charlie_id")
        self.assertIsNotNone(props_after)
        assert props_after is not None

        self.assertIn("age", props_after)
        self.assertNotIn(
            "location", props_after, "location 속성은 제거되어야 함"
        )

    def test_add_and_remove_labels_from_node(self):
        """
        add_labels_to_node -> 노드에 라벨 추가
        remove_labels_from_node -> 노드에서 라벨 제거
        """
        node = Node({"prop": "val"}, {self.test_label}, "test_node_lbl")
        self.conn.upsert_node(node)

        # 라벨 추가
        self.conn.add_labels_to_node("test_node_lbl", ["ExtraLabel"])

        # Cypher로 직접 라벨 목록 조회
        @with_session.readonly_transaction
        def check_labels(conn: Neo4jTestconnection, tx: ManagedTransaction):
            rec = tx.run(
                "MATCH (n:TESTUTIL {globalId:'test_node_lbl'}) RETURN labels(n) as lbls"
            ).single()
            return rec["lbls"] if rec else []

        labels_added = check_labels(self.conn)
        self.assertIn("TESTUTIL", labels_added)
        self.assertIn("ExtraLabel", labels_added)

        # 라벨 제거
        self.conn.remove_labels_from_node("test_node_lbl", ["ExtraLabel"])
        labels_removed = check_labels(self.conn)
        self.assertIn("TESTUTIL", labels_removed)
        self.assertNotIn("ExtraLabel", labels_removed)

    def test_link_nodes_and_relationship_properties(self):
        """
        link_nodes -> 두 노드를 rel_type으로 연결,
        update_relationship_properties -> 관계 속성 수정,
        get_relationship_properties -> 수정된 속성 확인,
        delete_relationship_by_global_id -> 관계 삭제
        """
        # upsert_node로 start/end 노드를 만든다
        n1 = Node({"name": "Node1"}, {self.test_label}, "n1_id")
        n2 = Node({"name": "Node2"}, {self.test_label}, "n2_id")
        self.conn.upsert_node(n1)
        self.conn.upsert_node(n2)

        # link_nodes -> globalId가 있는 노드를 rel_type으로 연결 (MERGE)
        self.conn.link_nodes(
            "n1_id", "n2_id", "LINKED", properties={"weight": 10}
        )

        # relationship의 globalId가 없는 상태 -> Neo4jConnection 레벨에서 "globalId"를 제어하지 않음
        # 관계 속성을 조회하려면, match_relationships나 find_nodes_in_relationship를 사용하거나
        # Cypher 직접 호출해야 한다. 여기서는 match_relationships를 사용.
        matched_rels = self.conn.match_relationships(
            rel_type="LINKED", property_filters={"weight": 10}
        )
        self.assertEqual(len(matched_rels), 1, "생성된 관계가 1개 있어야 함")
        # 관계 globalId가 없으므로 None
        self.assertIsNone(matched_rels[0].globalId)

        # update_relationship_properties( ) -> globalId 없는 관계는 match가 안 됨.
        # 대신 test_upsert_relationship에서는 globalId가 있을 때만 업데이트를 잘 확인했음.
        # 여기서는 간단히 merge된 관계를 Cypher로 직접 찾아서 업데이트하는 방법 시연
        @with_session.readwrite_transaction
        def update_link(conn: Neo4jTestconnection, tx: ManagedTransaction):
            tx.run(
                """
                MATCH (a:TESTUTIL {globalId:'n1_id'})-[r:LINKED]->(b:TESTUTIL {globalId:'n2_id'})
                SET r.weight = 99
                """
            )

        update_link(self.conn)

        # 다시 match해서 확인
        updated_rels = self.conn.match_relationships(
            "LINKED", {"weight": 99}
        )
        self.assertEqual(len(updated_rels), 1)

        # 관계 삭제. globalId가 없으므로 delete_relationship_by_global_id는 못쓰고,
        # delete_relationships_by_type로 속성 필터 써볼 수도 있음
        self.conn.delete_relationships_by_type("LINKED", {"weight": 99})
        after_del = self.conn.match_relationships("LINKED")
        self.assertEqual(len(after_del), 0, "관계가 삭제되어야 함")

    def test_count_nodes_and_relationships(self):
        """
        count_nodes / count_relationships로 라벨, 관계타입 개수를 세는 테스트
        """
        # 노드/관계 3개 정도 생성
        n1 = Node({}, {self.test_label}, "countA")
        n2 = Node({}, {self.test_label}, "countB")
        n3 = Node({}, {self.test_label}, "countC")
        self.conn.upsert_node(n1)
        self.conn.upsert_node(n2)
        self.conn.upsert_node(n3)

        self.conn.link_nodes("countA", "countB", "LINKED")
        self.conn.link_nodes("countB", "countC", "LINKED")

        # 노드 수 세기
        node_count = self.conn.count_nodes(self.test_label)
        self.assertEqual(node_count, 3, "테스트 라벨 노드가 3개 있어야 함")

        # 관계 수 세기
        rel_count = self.conn.count_relationships("LINKED")
        self.assertEqual(rel_count, 2, "LINKED 타입 관계가 2개 있어야 함")

    def test_find_nodes_in_relationship(self):
        """
        find_nodes_in_relationship: (start_node, relationship, end_node) 튜플로 반환
        """
        # 노드 2개, 관계 1개
        n1 = Node({"prop": "n1"}, {self.test_label}, "gidA")
        n2 = Node({"prop": "n2"}, {self.test_label}, "gidB")
        self.conn.upsert_node(n1)
        self.conn.upsert_node(n2)
        self.conn.link_nodes(
            "gidA", "gidB", "KNOWS", properties={"since": 2022}
        )

        # find_nodes_in_relationship
        result_triples = self.conn.find_nodes_in_relationship(
            rel_type="KNOWS", property_filters={"since": 2022}
        )
        self.assertEqual(len(result_triples), 1)
        start_node, relationship, end_node = result_triples[0]
        self.assertEqual(start_node.globalId, "gidA")
        self.assertEqual(end_node.globalId, "gidB")
        self.assertEqual(relationship.rel_type, "KNOWS")
        self.assertEqual(relationship["since"], 2022)


if __name__ == "__main__":
    unittest.main()
