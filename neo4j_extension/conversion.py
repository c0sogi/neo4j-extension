from datetime import date, datetime, time, timedelta
from typing import Dict as PyDict
from typing import List as PyList
from typing import Optional, Union

from .abc import Neo4jType
from .core import (
    Neo4jBoolean,
    Neo4jByteArray,
    Neo4jFloat,
    Neo4jInteger,
    Neo4jList,
    Neo4jMap,
    Neo4jNull,
    Neo4jString,
)
from .spatial import (
    Neo4jPoint,
)
from .temporal import (
    Neo4jDate,
    Neo4jDuration,
    Neo4jLocalDateTime,
    Neo4jLocalTime,
    Neo4jZonedDateTime,
    Neo4jZonedTime,
)


PythonType = Union[
    Union[
        None,
        bool,
        int,
        float,
        str,
        date,
        datetime,
        time,
        timedelta,
        bytes,
    ],
    PyList["PythonType"],
    PyDict[str, "PythonType"],
]


def convert_cystr_to_cytype(expr: str) -> Neo4jType:
    """
    임의의 Neo4j 리터럴/표현식을 파싱해 대응하는 Neo4jType 객체로 변환.
    """

    expr_strip = expr.strip().lower()

    # 1) null
    if expr_strip == "null":
        return Neo4jNull()

    # 2) boolean
    if expr_strip in ("true", "false"):
        return Neo4jBoolean.from_cypher(expr_strip)

    # 3) integer 시도
    try:
        # float문자열이면 int(...)에서 실패
        ival = int(expr_strip)
        return Neo4jInteger(ival)
    except ValueError:
        pass

    # 4) float
    try:
        return Neo4jFloat.from_cypher(expr_strip)
    except ValueError:
        pass

    # 5) string
    #    (원본 문자열로 다시 시도해야 하므로 expr.strip() 대신 raw)
    try:
        return Neo4jString.from_cypher(expr)
    except ValueError:
        pass

    # 6) date / time / datetime / duration / point / bytearray 등 함수
    #    (expr_strip 아닌 원본으로 전달)
    for cls_candidate in (
        Neo4jDate,
        Neo4jLocalTime,
        Neo4jLocalDateTime,
        Neo4jZonedTime,
        Neo4jZonedDateTime,
        Neo4jDuration,
        Neo4jPoint,
        Neo4jByteArray,
    ):
        try:
            return cls_candidate.from_cypher(expr)
        except ValueError:
            pass
        except NotImplementedError:
            pass

    # 7) list
    try:
        return Neo4jList.from_cypher(expr)
    except ValueError:
        pass

    # 8) map
    try:
        return Neo4jMap.from_cypher(expr)
    except ValueError:
        pass

    raise ValueError(
        f"Could not parse expression as any known Neo4j type: {expr}"
    )


def ensure_cypher_type(value: Union[Neo4jType, PythonType]) -> Neo4jType:
    """
    '이미 Neo4jType'이면 그대로 반환,
    그렇지 않다면 파이썬 기본 타입 등을 적절한 Neo4jType으로 변환.
    """

    if isinstance(value, Neo4jType):
        return value

    if value is None:
        return Neo4jNull()
    if isinstance(value, bool):
        return Neo4jBoolean(value)
    if isinstance(value, int):
        return Neo4jInteger(value)
    if isinstance(value, float):
        return Neo4jFloat(value)
    if isinstance(value, str):
        return Neo4jString(value)
    if isinstance(value, date) and not isinstance(value, datetime):
        return Neo4jDate(value)
    if isinstance(value, datetime):
        # datetime이지만 tzinfo가 None이면 LocalDateTime, 있으면 ZonedDateTime
        if value.tzinfo is None:
            return Neo4jLocalDateTime(value)
        else:
            return Neo4jZonedDateTime(value)
    if isinstance(value, time) and not isinstance(value, datetime):
        # time에 tzinfo가 있으면 ZonedTime, 없으면 LocalTime
        if value.tzinfo is None:
            return Neo4jLocalTime(value)
        else:
            return Neo4jZonedTime(value)
    if isinstance(value, timedelta):
        return Neo4jDuration(value)
    if isinstance(value, bytes):
        return Neo4jByteArray(value)

    # list -> Neo4jList (재귀 변환)
    if isinstance(value, list):
        converted = [ensure_cypher_type(v) for v in value]
        return Neo4jList(converted)

    # dict -> Neo4jMap (재귀 변환)
    if isinstance(value, dict):
        conv_map = {}
        for k, v in value.items():
            # key는 문자열이어야
            if not isinstance(k, str):
                raise TypeError(f"Map key must be str, got {k}")
            conv_map[k] = ensure_cypher_type(v)
        return Neo4jMap(conv_map)

    raise TypeError(f"[ensure_cypher_type] 변환 불가한 값: {repr(value)}")


def get_neo4j_property_type_name(val: Neo4jType) -> str:
    """
    프로퍼티로 저장 가능한지 판단할 때, type 이름 확인용
    """
    if isinstance(val, Neo4jNull):
        return "null"
    if isinstance(val, Neo4jBoolean):
        return "boolean"
    if isinstance(val, Neo4jInteger):
        return "integer"
    if isinstance(val, Neo4jFloat):
        return "float"
    if isinstance(val, Neo4jString):
        return "string"
    if isinstance(val, Neo4jDate):
        return "date"
    if isinstance(val, Neo4jLocalTime):
        return "time"
    if isinstance(val, Neo4jLocalDateTime):
        return "datetime"
    if isinstance(val, Neo4jZonedTime):
        return "time"
    if isinstance(val, Neo4jZonedDateTime):
        return "datetime"
    if isinstance(val, Neo4jDuration):
        return "duration"
    if isinstance(val, Neo4jPoint):
        return "point"
    if isinstance(val, Neo4jByteArray):
        return "bytearray"
    if isinstance(val, Neo4jList):
        return "list"
    # 그 밖은 property로 저장 불가 (MAP도 마찬가지로 직접 저장 X)
    return "other"


def cypher_value_to_python(value: Neo4jType) -> PythonType:
    """
    Neo4jType -> Python 기본 타입(또는 재귀 구조)으로 변환.
    """

    # null
    if isinstance(value, Neo4jNull):
        return None

    # list
    if isinstance(value, Neo4jList):
        return [cypher_value_to_python(v) for v in value.value]

    # map
    if isinstance(value, Neo4jMap):
        py_map = {}
        for k, v in value.value.items():
            py_map[k] = cypher_value_to_python(v)
        return py_map

    # 나머지는 .value 직접 반환
    return value.value


def entity_properties_to_dict(
    entity_props: dict[str, Neo4jType],
    keep_element_id: bool = False,
    element_id_val: Optional[str] = None,
) -> dict[str, PythonType]:
    """
    Node/Relationship의 properties(Neo4jType dict)를 모두 Python 기본값(dict)으로 변환.
    keep_element_id가 True면, element_id도 함께 store
    """
    result: dict[str, PythonType] = {}
    for k, v in entity_props.items():
        result[k] = cypher_value_to_python(v)
    if keep_element_id and element_id_val is not None:
        result["element_id"] = element_id_val
    return result
