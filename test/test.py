import unittest
from datetime import date, time, timedelta
from typing import Any

from neo4j import ManagedTransaction, Result

from neo4j_extension import Node, cypher_value_to_python, ensure_cypher_type
from neo4j_extension.connection import Neo4jConnection, with_session
from neo4j_extension.conversion import PythonType


class Neo4jTestconnection(Neo4jConnection):
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


class TestPersonOperations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = Neo4jTestconnection()
        cls.conn.clear_all_test_data()

    @classmethod
    def tearDownClass(cls):
        """테스트 클래스 종료 시 한 번만 실행"""
        if cls.conn:
            cls.conn.clear_all_test_data()  # 테스트 종료 시 테스트 데이터 정리
            cls.conn.close()

    def setUp(self):
        """각 테스트 메서드 실행 전에 실행"""
        self.conn.clear_all_test_data()

    def tearDown(self):
        """각 테스트 메서드 실행 후에 실행"""
        self.conn.clear_all_test_data()

    def test_create_person(self):
        """사람 생성 테스트"""
        # Given
        self.conn.create_person("John Doe", 30)

        # When
        persons: list[tuple[str, int]] = self.conn.get_all_person_age()
        if persons is None:
            self.fail("persons should not be None")

        # Then
        self.assertIsNotNone(persons, "persons should not be None")
        self.assertEqual(len(persons), 1, "should have exactly one person")
        self.assertIn(("John Doe", 30), persons)

    def test_get_all_persons(self):
        # Given
        test_data = [
            ("John Doe", 30),
            ("Jane Kim", 25),
        ]
        for name, age in test_data:
            self.conn.create_person(name, age)

        # When
        persons = self.conn.get_all_person_age()
        if persons is None:
            self.fail("persons should not be None")

        # Then
        self.assertIsNotNone(persons, "persons should not be None")
        self.assertEqual(len(persons), 2, "should have exactly two persons")
        for person in test_data:
            self.assertIn(person, persons)

    def test_unique_constraint(self):
        # Given
        self.conn.create_person("John Doe", 30)

        # When/Then
        with self.assertRaises(Exception):
            self.conn.create_person("John Doe", 40)

    def test_types(self) -> None:
        properties: dict[str, PythonType] = {
            "name": "Keanu Reeves",
            "age": 42,
            "height": 180.5,
            "active": True,
            "crime": None,
            "byte_data": b"Hello world",
            "tags": ["hero", "matrix"],
            "duration": timedelta(weeks=1, days=3, hours=12),
            "date": date(2021, 10, 1),
            "time": time(12, 34, 56, 789000),
        }

        for prop_val in properties.values():
            cypher_type = ensure_cypher_type(prop_val)
            cypher: str = cypher_type.to_cypher()
            parsed = cypher_value_to_python(cypher_type.from_cypher(cypher))
            self.assertEqual(parsed, prop_val)

        # Node 생성 시: 파이썬 기본 타입 섞어서 넘기기
        node = Node(
            element_id="test_node",
            properties=properties,
            labels={"Person", "Actor", "TEST"},
        )
        print(node.to_cypher_map())
        self.conn.upsert_node(node)

        # 만약 ListValue가 heterogeneous 하다면 (int+str):
        node = Node(
            element_id="node_bad",
            labels={"Test"},
            properties={"mixed_list": [1, "two"]},  # 섞여 있음
        )
        self.assertRaises(
            ValueError,
            node.to_cypher_map,
        )


if __name__ == "__main__":
    unittest.main()
