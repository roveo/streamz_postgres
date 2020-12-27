import json
from datetime import date, datetime, time
from functools import wraps

from streamz_postgres.pg.data_types_info import DataTypesInfo


def noneable(f):
    """Return None if the input value is an empty str."""

    @wraps(f)
    def inner(s: str):
        if s == "":
            return None
        return f(s)

    return inner


class DataType:
    name = None
    mapping = {}

    @classmethod
    def convert(cls, s: str):
        """Convert a string representation of data into a concrete data type. The
        default for unknown data types is to leave it a str.
        """
        return cls.mapping.get(s, s)


@DataTypesInfo.register_data_type
class Bool(DataType):
    name = "bool"
    mapping = {"": None, "t": True, "f": False}


class IntegerType(DataType):
    @staticmethod
    @noneable
    def convert(s: str) -> int:
        return int(s)


@DataTypesInfo.register_data_type
class Int2(IntegerType):
    name = "int2"


@DataTypesInfo.register_data_type
class Int4(IntegerType):
    name = "int4"


@DataTypesInfo.register_data_type
class Int8(IntegerType):
    name = "int8"


@DataTypesInfo.register_data_type
class Oid(IntegerType):
    name = "oid"


@DataTypesInfo.register_data_type
class Tid(IntegerType):
    name = "tid"


@DataTypesInfo.register_data_type
class Xid(IntegerType):
    name = "xid"


@DataTypesInfo.register_data_type
class Cid(IntegerType):
    name = "cid"


@DataTypesInfo.register_data_type
class Json(DataType):
    name = "json"

    @staticmethod
    @noneable
    def convert(s: str):
        return json.loads(s)


class FloatType(DataType):
    @staticmethod
    @noneable
    def convert(s: str) -> float:
        return float(s)


@DataTypesInfo.register_data_type
class Float4(FloatType):
    name = "float4"


@DataTypesInfo.register_data_type
class Float8(FloatType):
    name = "float8"


@DataTypesInfo.register_data_type
class Numeric(FloatType):
    name = "numeric"


@DataTypesInfo.register_data_type
class Date(DataType):
    name = "data"

    @staticmethod
    @noneable
    def convert(s: str) -> date:
        return date.fromisoformat(s)


@DataTypesInfo.register_data_type
class Time(DataType):
    name = "time"

    @staticmethod
    @noneable
    def convert(s: str) -> time:
        return time.fromisoformat(s)


@DataTypesInfo.register_data_type
class Timestamp(DataType):
    name = "timestamp"

    @staticmethod
    @noneable
    def convert(s: str) -> datetime:
        return datetime.fromisoformat(s)


@DataTypesInfo.register_data_type
class Timestamptz(Timestamp):
    name = "timestamptz"
