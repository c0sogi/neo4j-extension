from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from typing import LiteralString, Optional, cast

from .abc import Neo4jType

###############################################################################
# DATE  (date('YYYY-MM-DD'))
###############################################################################

DATE_REGEX = re.compile(
    r"""^\s*date\(\s*(['"])([0-9+\-]{1,}-[0-9]{1,2}-[0-9]{1,2})\1\s*\)\s*$""",
    re.IGNORECASE,
)


class Neo4jDate(Neo4jType[date]):
    """
    Corresponds to Neo4j's DATE type: ISO-8601 date (YYYY-MM-DD).
    """

    def __init__(self, value: date):
        self.value = value

    def to_cypher(self) -> LiteralString:
        return cast(LiteralString, f"date('{self.value.isoformat()}')")

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jDate:
        m = DATE_REGEX.match(cypher_str)
        if not m:
            raise ValueError(f"Invalid Neo4j date literal: {cypher_str}")
        iso_str = m.group(2)
        try:
            d = date.fromisoformat(iso_str)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {iso_str}") from e
        return cls(d)


###############################################################################
# LOCAL TIME  (time('HH:MM:SS[.fff]'))
###############################################################################

TIME_REGEX = re.compile(
    r"""^\s*time\(\s*(['"])([0-9:\.]+)\1\s*\)\s*$""", re.IGNORECASE
)


class Neo4jLocalTime(Neo4jType[time]):
    """
    Corresponds to Neo4j's LOCAL TIME type (no timezone).
    """

    def __init__(self, value: time):
        self.value = value

    def to_cypher(self) -> LiteralString:
        return cast(LiteralString, f"time('{self.value.isoformat()}')")

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jLocalTime:
        m = TIME_REGEX.match(cypher_str)
        if not m:
            raise ValueError(
                f"Invalid Neo4j local time literal: {cypher_str}"
            )
        iso_str = m.group(2)
        try:
            t = time.fromisoformat(iso_str)
        except ValueError as e:
            raise ValueError(f"Invalid local time format: {iso_str}") from e
        # tzinfo 없는지 확인
        if t.tzinfo is not None:
            raise ValueError("Expected local time (no tzinfo).")
        return cls(t)


###############################################################################
# LOCAL DATETIME (datetime('YYYY-MM-DDTHH:MM:SS[.fff]'))
###############################################################################

LOCAL_DATETIME_REGEX = re.compile(
    r"""^\s*datetime\(\s*(['"])([0-9+\-T:\.]+)\1\s*\)\s*$""",
    re.IGNORECASE,
)


class Neo4jLocalDateTime(Neo4jType[datetime]):
    """
    Corresponds to Neo4j's LOCAL DATETIME type (no timezone).
    """

    def __init__(self, value: datetime):
        if value.tzinfo is not None:
            raise ValueError("LocalDateTime should not have tzinfo.")
        self.value = value

    def to_cypher(self) -> LiteralString:
        return cast(LiteralString, f"datetime('{self.value.isoformat()}')")

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jLocalDateTime:
        m = LOCAL_DATETIME_REGEX.match(cypher_str)
        if not m:
            raise ValueError(f"Invalid local datetime literal: {cypher_str}")
        iso_str = m.group(2)
        try:
            dt = datetime.fromisoformat(iso_str)
        except ValueError as e:
            raise ValueError(
                f"Invalid local datetime format: {iso_str}"
            ) from e
        if dt.tzinfo is not None:
            raise ValueError("Expected local datetime (no tz).")
        return cls(dt)


###############################################################################
# ZONED TIME  (time('HH:MM:SS[.fff][+HH:MM]'))
###############################################################################

ZONED_TIME_REGEX = re.compile(
    r"""^\s*time\(\s*(['"])([0-9:\.\+\-]+)\1\s*\)\s*$""",
    re.IGNORECASE,
)


class Neo4jZonedTime(Neo4jType[time]):
    """
    Corresponds to Neo4j's ZONED TIME type (time with timezone offset).
    """

    def __init__(self, value: time):
        # Assert that the time has a timezone offset
        if value.tzinfo is None:
            raise ValueError("ZonedTime requires a tzinfo (offset).")
        self.value = value

    def to_cypher(self) -> LiteralString:
        return cast(LiteralString, f"time('{self.value.isoformat()}')")

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jZonedTime:
        m = ZONED_TIME_REGEX.match(cypher_str)
        if not m:
            raise ValueError(
                f"Invalid Neo4j zoned time literal: {cypher_str}"
            )
        iso_str = m.group(2)
        try:
            t = time.fromisoformat(iso_str)
        except ValueError as e:
            raise ValueError(f"Invalid zoned time format: {iso_str}") from e
        if t.tzinfo is None:
            raise ValueError("Zoned time string must have offset.")
        return cls(t)


###############################################################################
# ZONED DATETIME  (datetime('YYYY-MM-DDTHH:MM:SS[.fff][+HH:MM]'))
###############################################################################

ZONED_DATETIME_REGEX = re.compile(
    r"""^\s*datetime\(\s*(['"])([0-9+\-T:\.]+(?:Z|[+\-][0-9:]+))\1\s*\)\s*$""",
    re.IGNORECASE,
)


class Neo4jZonedDateTime(Neo4jType[datetime]):
    """
    Corresponds to Neo4j's ZONED DATETIME type (datetime with timezone offset).
    """

    def __init__(self, value: datetime):
        if value.tzinfo is None:
            raise ValueError("ZonedDateTime requires a tzinfo.")
        self.value = value

    def to_cypher(self) -> LiteralString:
        return cast(LiteralString, f"datetime('{self.value.isoformat()}')")

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jZonedDateTime:
        m = ZONED_DATETIME_REGEX.match(cypher_str)
        if not m:
            raise ValueError(
                f"Invalid Neo4j zoned datetime literal: {cypher_str}"
            )
        iso_str = m.group(2)
        try:
            dt = datetime.fromisoformat(iso_str)
        except ValueError as e:
            raise ValueError(
                f"Invalid zoned datetime format: {iso_str}"
            ) from e
        if dt.tzinfo is None:
            raise ValueError("Zoned datetime must have offset.")
        return cls(dt)


###############################################################################
# DURATION  (duration('P1Y2M3DT4H5M6.7S'))
###############################################################################

DURATION_REGEX = re.compile(
    r"""^\s*duration\(\s*(['"])(P.*)\1\s*\)\s*$""", re.IGNORECASE
)

DURATION_ISO_REGEX = re.compile(
    r"""
    ^
    P
    (?:(?P<years>   [+-]?\d+(?:\.\d+)? )Y)?
    (?:(?P<months>  [+-]?\d+(?:\.\d+)? )M)?
    (?:(?P<days>    [+-]?\d+(?:\.\d+)? )D)?
    (?:T
        (?:(?P<hours>   [+-]?\d+(?:\.\d+)? )H)?
        (?:(?P<minutes> [+-]?\d+(?:\.\d+)? )M)?
        (?:(?P<seconds> [+-]?\d+(?:\.\d+)? )S)?
    )?
    $
    """,
    re.IGNORECASE | re.VERBOSE,
)


class Neo4jDuration(Neo4jType[timedelta]):
    """
    Corresponds to Neo4j's DURATION type.
    """

    def __init__(self, value: timedelta):
        self.value = value

    def to_cypher(self) -> LiteralString:
        total_seconds = int(self.value.total_seconds())
        micros = self.value.microseconds
        sign = -1 if total_seconds < 0 else 1
        total_seconds = abs(total_seconds)
        days = total_seconds // 86400
        remain = total_seconds % 86400
        hours = remain // 3600
        remain %= 3600
        minutes = remain // 60
        seconds = remain % 60

        frac: str = ""
        if micros > 0:
            frac_val = micros / 1_000_000
            frac = f"{frac_val}".lstrip("0")

        base: str = f"P{days}DT{hours}H{minutes}M{seconds}{frac}S"
        return cast(
            LiteralString, f"duration('{'-' if sign < 0 else ''}{base}')"
        )

    @classmethod
    def from_cypher(cls, cypher_str: str) -> Neo4jDuration:
        s = cypher_str.strip()
        pattern = (
            r"^duration\(\s*'(?P<sign>[+-])?P"
            r"(?:(?P<years>\d+(?:\.\d+)?)Y)?"
            r"(?:(?P<months>\d+(?:\.\d+)?)M)?"
            r"(?:(?P<weeks>\d+(?:\.\d+)?)W)?"
            r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
            r"(?:T(?:(?P<hours>\d+(?:\.\d+)?)H)?"
            r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
            r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
            r"'\s*\)$"
        )
        m = re.match(pattern, s)
        if not m:
            raise ValueError(
                f"[DurationValue] duration(...), Not a literal: {cypher_str}"
            )

        def to_f(raw: Optional[str]) -> float:
            return float(raw) if raw else 0.0

        sign_str = m.group("sign")
        sign = -1 if sign_str == "-" else 1
        years = to_f(m.group("years"))
        months = to_f(m.group("months"))
        weeks = to_f(m.group("weeks"))
        days_ = to_f(m.group("days"))
        hours = to_f(m.group("hours"))
        minutes = to_f(m.group("minutes"))
        seconds = to_f(m.group("seconds"))

        total_days = (years * 360) + (months * 30) + (weeks * 7) + days_
        total_seconds = (
            total_days * 86400 + hours * 3600 + minutes * 60 + seconds
        )
        total_seconds *= sign

        td = timedelta(seconds=total_seconds)
        return cls(td)
