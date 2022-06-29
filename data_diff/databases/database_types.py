import decimal
from abc import ABC, abstractmethod
from typing import Sequence, Optional, Tuple, Union, Dict, Any
from datetime import datetime

from runtype import dataclass

from data_diff.utils import ArithUUID


DbPath = Tuple[str, ...]
DbKey = Union[int, str, bytes, ArithUUID]
DbTime = datetime


class ColType:
    supported = True
    pass


@dataclass
class PrecisionType(ColType):
    precision: int
    rounds: bool


class TemporalType(PrecisionType):
    pass


class Timestamp(TemporalType):
    pass


class TimestampTZ(TemporalType):
    pass


class Datetime(TemporalType):
    pass


@dataclass
class NumericType(ColType):
    # 'precision' signifies how many fractional digits (after the dot) we want to compare
    precision: int


class FractionalType(NumericType):
    pass


class Float(FractionalType):
    pass


class Decimal(FractionalType):
    @property
    def python_type(self) -> type:
        if self.precision == 0:
            return int
        return decimal.Decimal


class StringType(ColType):
    pass


class IKey(ABC):
    "Interface for ColType, for using a column as a key in data-diff"
    python_type: type


class ColType_UUID(StringType, IKey):
    python_type = ArithUUID


@dataclass
class Text(StringType):
    supported = False


@dataclass
class Integer(NumericType, IKey):
    precision: int = 0
    python_type: type = int

    def __post_init__(self):
        assert self.precision == 0


@dataclass
class UnknownColType(ColType):
    text: str

    supported = False


class AbstractDatabase(ABC):
    @abstractmethod
    def quote(self, s: str):
        "Quote SQL name (implementation specific)"
        ...

    @abstractmethod
    def to_string(self, s: str) -> str:
        "Provide SQL for casting a column to string"
        ...

    @abstractmethod
    def md5_to_int(self, s: str) -> str:
        "Provide SQL for computing md5 and returning an int"
        ...

    @abstractmethod
    def offset_limit(self, offset: Optional[int] = None, limit: Optional[int] = None):
        ...

    @abstractmethod
    def _query(self, sql_code: str) -> list:
        "Send query to database and return result"
        ...

    @abstractmethod
    def select_table_schema(self, path: DbPath) -> str:
        "Provide SQL for selecting the table schema as (name, type, date_prec, num_prec)"
        ...

    @abstractmethod
    def query_table_schema(self, path: DbPath, filter_columns: Optional[Sequence[str]] = None) -> Dict[str, ColType]:
        "Query the table for its schema for table in 'path', and return {column: type}"
        ...

    @abstractmethod
    def parse_table_name(self, name: str) -> DbPath:
        "Parse the given table name into a DbPath"
        ...

    @abstractmethod
    def close(self):
        "Close connection(s) to the database instance. Querying will stop functioning."
        ...

    @abstractmethod
    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized timestamp.

        The returned expression must accept any SQL datetime/timestamp, and return a string.

        Date format: ``YYYY-MM-DD HH:mm:SS.FFFFFF``

        Precision of dates should be rounded up/down according to coltype.rounds
        """
        ...

    @abstractmethod
    def normalize_number(self, value: str, coltype: FractionalType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized number.

        The returned expression must accept any SQL int/numeric/float, and return a string.

        Floats/Decimals are expected in the format
        "I.P"

        Where I is the integer part of the number (as many digits as necessary),
        and must be at least one digit (0).
        P is the fractional digits, the amount of which is specified with
        coltype.precision. Trailing zeroes may be necessary.
        If P is 0, the dot is omitted.

        Note: We use 'precision' differently than most databases. For decimals,
        it's the same as ``numeric_scale``, and for floats, who use binary precision,
        it can be calculated as ``log10(2**numeric_precision)``.
        """
        ...

    @abstractmethod
    def normalize_uuid(self, value: str, coltype: ColType_UUID) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized uuid.

        i.e. just makes sure there is no trailing whitespace.
        """
        ...

    def normalize_value_by_type(self, value: str, coltype: ColType) -> str:
        """Creates an SQL expression, that converts 'value' to a normalized representation.

        The returned expression must accept any SQL value, and return a string.

        The default implementation dispatches to a method according to `coltype`:

        ::

            TemporalType    -> normalize_timestamp()
            FractionalType  -> normalize_number()
            *else*          -> to_string()

            (`Integer` falls in the *else* category)

        """
        if isinstance(coltype, TemporalType):
            return self.normalize_timestamp(value, coltype)
        elif isinstance(coltype, FractionalType):
            return self.normalize_number(value, coltype)
        elif isinstance(coltype, ColType_UUID):
            return self.normalize_uuid(value, coltype)
        return self.to_string(value)

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        ...


class Schema(ABC):
    @abstractmethod
    def get_key(self, key: str) -> str:
        ...

    @abstractmethod
    def __getitem__(self, key: str) -> ColType:
        ...

    @abstractmethod
    def __setitem__(self, key: str, value):
        ...

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        ...


class Schema_CaseSensitive(dict, Schema):
    def get_key(self, key):
        return key


class Schema_CaseInsensitive(Schema):
    def __init__(self, initial):
        self._dict = {k.lower(): (k, v) for k, v in dict(initial).items()}

    def get_key(self, key: str) -> str:
        return self._dict[key.lower()][0]

    def __getitem__(self, key: str) -> ColType:
        return self._dict[key.lower()][1]

    def __setitem__(self, key: str, value):
        k = key.lower()
        if k in self._dict:
            key = self._dict[k][0]
        self._dict[k] = key, value

    def __contains__(self, key):
        return key.lower() in self._dict
