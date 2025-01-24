from __future__ import annotations

import base64
import re
from typing import (
    Dict as PyDict,
)
from typing import (
    List as PyList,
)

from ._utils import _split_by_comma_top_level, _tokenize_cypher_expression
from .abc import Neo4jType

LIST_REGEX = re.compile(r"""^\s*\[\s*(.*)\s*\]\s*$""", re.DOTALL)
MAP_REGEX = re.compile(r"""^\s*\{\s*(.*)\s*\}\s*$""", re.DOTALL)


###############################################################################
# Neo4jNull (null)
###############################################################################


class Neo4jNull(Neo4jType[None]):
    """
    Neo4j 상의 null 값 표현. (v1의 NullValue에 해당)
    """

    value: None = None

    def to_cypher(self) -> str:
        return "null"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jNull:
        if cypher_str.strip().lower() == "null":
            return cls()
        raise ValueError(f"Not a valid null literal: {cypher_str}")


###############################################################################
# BOOLEAN
###############################################################################


class Neo4jBoolean(Neo4jType[bool]):
    """
    Corresponds to Neo4j's BOOLEAN type (true / false).
    """

    def __init__(self, value: bool):
        self.value = bool(value)

    def to_cypher(self) -> str:
        return "true" if self.value else "false"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jBoolean:
        s = cypher_str.strip().lower()
        if s == "true":
            return cls(True)
        elif s == "false":
            return cls(False)
        raise ValueError(f"Invalid Neo4j boolean string: {cypher_str}")


###############################################################################
# INTEGER (64-bit)
###############################################################################


class Neo4jInteger(Neo4jType[int]):
    """
    Corresponds to Neo4j's INTEGER type (64-bit signed).
    """

    def __init__(self, value: int):
        # 64-bit 범위 체크(필요하다면)
        if value < -(2**63) or value > 2**63 - 1:
            raise OverflowError("Neo4j INTEGER out of 64-bit range.")
        self.value = value

    def to_cypher(self) -> str:
        return str(self.value)

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jInteger:
        val = int(cypher_str.strip())
        return cls(val)


###############################################################################
# FLOAT (64-bit double precision)
###############################################################################


class Neo4jFloat(Neo4jType[float]):
    """
    Corresponds to Neo4j's FLOAT type (64-bit).
    """

    def __init__(self, value: float):
        self.value = float(value)

    def to_cypher(self) -> str:
        return str(self.value)

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jFloat:
        s = cypher_str.strip().lower()
        # Neo4j에서 nan, infinity, -infinity 등도 허용될 수 있음
        if s == "nan":
            return cls(float("nan"))
        elif s == "infinity":
            return cls(float("inf"))
        elif s == "-infinity":
            return cls(float("-inf"))

        val = float(s)  # ValueError 가능
        return cls(val)


###############################################################################
# STRING
###############################################################################


class Neo4jString(Neo4jType[str]):
    """
    Corresponds to Neo4j's STRING type.
    """

    STRING_REGEX = re.compile(
        r"""^\s*'((?:\\.|''|[^'\\])*)'\s*$""", re.DOTALL
    )

    def __init__(self, value: str):
        self.value = value

    def to_cypher(self) -> str:
        # 내부 ' -> '' 치환, 역슬래시 등 이스케이프
        escaped = self.value
        escaped = escaped.replace("\\", "\\\\")
        escaped = escaped.replace("'", "''")
        return f"'{escaped}'"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jString:
        match = cls.STRING_REGEX.match(cypher_str)
        if not match:
            raise ValueError(f"Invalid Neo4j string literal: {cypher_str}")

        content = match.group(1)

        # '' -> '
        content = content.replace("''", "'")
        # 일부 이스케이프 시퀀스 처리(예시)
        content = content.replace("\\n", "\n")
        content = content.replace("\\t", "\t")
        content = content.replace("\\r", "\r")
        content = content.replace("\\b", "\b")
        content = content.replace("\\f", "\f")
        content = content.replace("\\\\", "\\")

        return cls(content)


###############################################################################
# BYTE ARRAY (Neo4j first-class가 아님, pass-through)
###############################################################################


class Neo4jByteArray(Neo4jType[bytes]):
    """
    Neo4j에 byte array를 넘길 수 있으나, Neo4j literal은 공식 문법이 없다.
    여기서는 'bytearray("...")' 식으로 가정하고 base64 인코딩/디코딩을 해본다.
    """

    def __init__(self, value: bytes):
        self.value = value

    def to_cypher(self) -> str:
        encoded = base64.b64encode(self.value).decode("ascii")
        return f"bytearray('{encoded}')"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jByteArray:
        s = cypher_str.strip()
        m = re.match(r"^bytearray\s*\(\s*'([^']*)'\s*\)$", s)
        if not m:
            raise ValueError(
                f"Invalid Neo4j bytearray literal: {cypher_str}"
            )
        b64 = m.group(1)
        data = base64.b64decode(b64.encode("ascii"))
        return cls(data)


###############################################################################
# Neo4jList
###############################################################################


class Neo4jList(Neo4jType[PyList[Neo4jType]]):
    """
    Represents a Neo4j LIST type: [elem0, elem1, ...].
    """

    def __init__(self, value: PyList[Neo4jType]):
        self.value = value

    def to_cypher(self) -> str:
        elems = [elem.to_cypher() for elem in self.value]
        return "[" + ", ".join(elems) + "]"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jList:
        from .conversion import convert_cypher_to_neo4j

        m = LIST_REGEX.match(cypher_str)
        if not m:
            raise ValueError(f"Invalid Neo4j list literal: {cypher_str}")
        inner = m.group(1).strip()
        if not inner:
            return cls([])

        tokens = _tokenize_cypher_expression(inner)
        elements_str_list = _split_by_comma_top_level(tokens)
        parsed_elems = [
            convert_cypher_to_neo4j(elem_str)
            for elem_str in elements_str_list
        ]
        return cls(parsed_elems)

    def is_storable_as_property(self) -> bool:
        """
        Neo4j에 property로 저장 가능한 리스트인지(동질 타입 + null 없음 + 중첩 불가 등) 검사.
        (v1의 ListValue.is_storable_as_property 참고)
        """
        from .conversion import get_neo4j_property_type_name

        if not self.value:
            return True  # 빈 리스트는 가능

        # 모든 원소의 타입 이름을 확인
        type_list = []
        for val in self.value:
            tname = get_neo4j_property_type_name(val)
            if tname == "null":
                return False
            if tname == "list":
                # 중첩 리스트 불가
                return False
            type_list.append(tname)

        # 모두 동일 타입인가?
        first = type_list[0]
        for other_type in type_list[1:]:
            if other_type != first:
                return False
        return True


###############################################################################
# Neo4jMap
###############################################################################


class Neo4jMap(Neo4jType[PyDict[str, Neo4jType]]):
    """
    Represents a Neo4j MAP: { key: value, ... }.
    """

    def __init__(self, value: PyDict[str, Neo4jType]):
        self.value = value

    def to_cypher(self) -> str:
        parts = []
        for k, v in self.value.items():
            # key는 안전하게 문자열 리터럴로
            k_cypher = Neo4jString(k).to_cypher()
            v_cypher = v.to_cypher()
            parts.append(f"{k_cypher}: {v_cypher}")
        return "{" + ", ".join(parts) + "}"

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jMap:
        from .conversion import convert_cypher_to_neo4j

        m = MAP_REGEX.match(cypher_str)
        if not m:
            raise ValueError(f"Invalid Neo4j map literal: {cypher_str}")
        inner = m.group(1).strip()
        if not inner:
            return cls({})

        tokens = _tokenize_cypher_expression(inner)
        elements_str_list = _split_by_comma_top_level(tokens)
        result: PyDict[str, Neo4jType] = {}

        for pair_str in elements_str_list:
            pair_tokens = _tokenize_cypher_expression(pair_str)
            try:
                colon_index = pair_tokens.index(":")
            except ValueError:
                raise ValueError(f"Invalid map entry (no colon): {pair_str}")
            key_tokens = pair_tokens[:colon_index]
            val_tokens = pair_tokens[colon_index + 1 :]

            key_str = "".join(key_tokens).strip()
            # key가 식별자 형태인지, string literal인지
            if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", key_str):
                # 식별자면 그대로 key 사용
                key = key_str
            else:
                # 아니면 Neo4jString 파싱 시도
                key_val = Neo4jString.from_cypher(key_str)
                key = key_val.value

            val_str = "".join(val_tokens).strip()
            val_obj = convert_cypher_to_neo4j(val_str)

            result[key] = val_obj

        return cls(result)
