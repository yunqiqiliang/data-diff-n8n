import time
from typing import Container, Dict, List, Optional, Sequence, Tuple
import logging
from itertools import product

import attrs
from typing_extensions import Self

from data_diff.utils import safezip, Vector
from data_diff.utils import ArithString, split_space
from data_diff.databases.base import Database
from data_diff.abcs.database_types import DbPath, DbKey, DbTime, IKey
from data_diff.schema import RawColumnInfo, Schema, create_schema
from data_diff.queries.extras import Checksum
from data_diff.queries.api import Count, SKIP, table, this, Expr, min_, max_, Code
from data_diff.queries.extras import ApplyFuncAndNormalizeAsString, NormalizeAsString

logger = logging.getLogger("table_segment")

RECOMMENDED_CHECKSUM_DURATION = 20


def split_key_space(min_key: DbKey, max_key: DbKey, count: int) -> List[DbKey]:
    assert min_key < max_key

    if max_key - min_key <= count:
        count = 1

    if isinstance(min_key, ArithString):
        assert type(min_key) is type(max_key)
        checkpoints = min_key.range(max_key, count)
    else:
        checkpoints = split_space(min_key, max_key, count)

    assert all(min_key < x < max_key for x in checkpoints)
    return [min_key] + checkpoints + [max_key]


def int_product(nums: List[int]) -> int:
    p = 1
    for n in nums:
        p *= n
    return p


def split_compound_key_space(mn: Vector, mx: Vector, count: int) -> List[List[DbKey]]:
    """Returns a list of split-points for each key dimension, essentially returning an N-dimensional grid of split points."""
    return [split_key_space(mn_k, mx_k, count) for mn_k, mx_k in safezip(mn, mx)]


def create_mesh_from_points(*values_per_dim: list) -> List[Tuple[Vector, Vector]]:
    """Given a list of values along each axis of N dimensional space,
    return an array of boxes whose start-points & end-points align with the given values,
    and together consitute a mesh filling that space entirely (within the bounds of the given values).

    Assumes given values are already ordered ascending.

    len(boxes) == ∏i( len(i)-1 )

    Example:
        ::
            >>> d1 = 'a', 'b', 'c'
            >>> d2 = 1, 2, 3
            >>> d3 = 'X', 'Y'
            >>> create_mesh_from_points(d1, d2, d3)
            [
                [('a', 1, 'X'), ('b', 2, 'Y')],
                [('a', 2, 'X'), ('b', 3, 'Y')],
                [('b', 1, 'X'), ('c', 2, 'Y')],
                [('b', 2, 'X'), ('c', 3, 'Y')]
            ]
    """
    assert all(len(v) >= 2 for v in values_per_dim), values_per_dim

    # Create tuples of (v1, v2) for each pair of adjacent values
    ranges = [list(zip(values[:-1], values[1:])) for values in values_per_dim]

    assert all(a <= b for r in ranges for a, b in r)

    # Create a product of all the ranges
    res = [tuple(Vector(a) for a in safezip(*r)) for r in product(*ranges)]

    expected_len = int_product(len(v) - 1 for v in values_per_dim)
    assert len(res) == expected_len, (len(res), expected_len)
    return res


@attrs.define(frozen=True)
class TableSegment:
    """Signifies a segment of rows (and selected columns) within a table

    Parameters:
        database (Database): Database instance. See :meth:`connect`
        table_path (:data:`DbPath`): Path to table in form of a tuple. e.g. `('my_dataset', 'table_name')`
        key_columns (Tuple[str]): Name of the key column, which uniquely identifies each row (usually id)
        update_column (str, optional): Name of updated column, which signals that rows changed.
                                       Usually updated_at or last_update. Used by `min_update` and `max_update`.
        extra_columns (Tuple[str, ...], optional): Extra columns to compare
        min_key (:data:`Vector`, optional): Lowest key value, used to restrict the segment
        max_key (:data:`Vector`, optional): Highest key value, used to restrict the segment
        min_update (:data:`DbTime`, optional): Lowest update_column value, used to restrict the segment
        max_update (:data:`DbTime`, optional): Highest update_column value, used to restrict the segment
        where (str, optional): An additional 'where' expression to restrict the search space.

        case_sensitive (bool): If false, the case of column names will adjust according to the schema. Default is true.
        
        sample_size (int, optional): Number of rows to sample for comparison
        sampling_method (str, optional): Sampling method - 'LIMIT', 'SYSTEM', 'BERNOULLI', or database-specific
        sampling_percent (float, optional): Percentage of rows to sample (0-100)

    """

    # Location of table
    database: Database
    table_path: DbPath

    # Columns
    key_columns: Tuple[str, ...]
    update_column: Optional[str] = None
    extra_columns: Tuple[str, ...] = ()
    ignored_columns: Container[str] = frozenset()

    # Restrict the segment
    min_key: Optional[Vector] = None
    max_key: Optional[Vector] = None
    min_update: Optional[DbTime] = None
    max_update: Optional[DbTime] = None
    where: Optional[str] = None
    
    # Sampling parameters
    sample_size: Optional[int] = None
    sampling_method: str = "LIMIT"
    sampling_percent: Optional[float] = None

    case_sensitive: Optional[bool] = True
    _schema: Optional[Schema] = None

    def __attrs_post_init__(self) -> None:
        if not self.update_column and (self.min_update or self.max_update):
            raise ValueError("Error: the min_update/max_update feature requires 'update_column' to be set.")

        if self.min_key is not None and self.max_key is not None and self.min_key >= self.max_key:
            raise ValueError(f"Error: min_key expected to be smaller than max_key! ({self.min_key} >= {self.max_key})")

        if self.min_update is not None and self.max_update is not None and self.min_update >= self.max_update:
            raise ValueError(
                f"Error: min_update expected to be smaller than max_update! ({self.min_update} >= {self.max_update})"
            )

    def _where(self) -> Optional[str]:
        return f"({self.where})" if self.where else None

    def _with_raw_schema(self, raw_schema: Dict[str, RawColumnInfo]) -> Self:
        schema = self.database._process_table_schema(self.table_path, raw_schema, self.relevant_columns, self._where())
        return self.new(schema=create_schema(self.database.name, self.table_path, schema, self.case_sensitive))

    def with_schema(self) -> Self:
        "Queries the table schema from the database, and returns a new instance of TableSegment, with a schema."
        if self._schema:
            return self

        return self._with_raw_schema(self.database.query_table_schema(self.table_path))

    def get_schema(self) -> Dict[str, RawColumnInfo]:
        return self.database.query_table_schema(self.table_path)

    def _make_key_range(self):
        if self.min_key is not None:
            for mn, k in safezip(self.min_key, self.key_columns):
                yield mn <= this[k]
        if self.max_key is not None:
            for k, mx in safezip(self.key_columns, self.max_key):
                yield this[k] < mx

    def _make_update_range(self):
        if self.min_update is not None:
            yield self.min_update <= this[self.update_column]
        if self.max_update is not None:
            yield this[self.update_column] < self.max_update

    @property
    def source_table(self):
        return table(*self.table_path, schema=self._schema)

    def make_select(self):
        query = self.source_table.where(
            *self._make_key_range(), *self._make_update_range(), Code(self._where()) if self.where else SKIP
        )
        
        # Apply sampling if configured
        if self.sample_size or self.sampling_percent:
            query = self._apply_sampling(query)
        
        return query
    
    def _apply_sampling(self, query):
        """Apply sampling to the query based on database type and sampling method"""
        db_name = self.database.name.lower()
        
        # Check if deterministic sampling is requested
        if self.sampling_method.upper() == "DETERMINISTIC":
            return self._apply_deterministic_sampling(query)
        
        if self.sampling_percent:
            # Percentage-based sampling
            if db_name in ("postgresql", "postgres"):
                # PostgreSQL supports TABLESAMPLE SYSTEM and BERNOULLI
                method = self.sampling_method.upper()
                if method not in ("SYSTEM", "BERNOULLI"):
                    method = "SYSTEM"  # Default to SYSTEM for PostgreSQL
                return Code(f"({query}) TABLESAMPLE {method} ({self.sampling_percent})")
            
            elif db_name == "clickzetta":
                # ClickZetta supports TABLESAMPLE with percentage
                return Code(f"({query}) TABLESAMPLE ({self.sampling_percent} PERCENT)")
            
            elif db_name == "clickhouse":
                # ClickHouse uses SAMPLE with decimal (0-1) or rows
                decimal_percent = self.sampling_percent / 100.0
                return Code(f"({query}) SAMPLE {decimal_percent}")
            
            elif db_name in ("mysql", "mariadb"):
                # MySQL doesn't have TABLESAMPLE, use ORDER BY RAND() with LIMIT
                # Note: This is expensive for large tables
                row_count = int(self.count() * self.sampling_percent / 100)
                return Code(f"({query} ORDER BY RAND() LIMIT {row_count})")
            
            elif db_name == "snowflake":
                # Snowflake supports SAMPLE
                return Code(f"({query}) SAMPLE ({self.sampling_percent})")
            
            elif db_name == "bigquery":
                # BigQuery supports TABLESAMPLE
                return Code(f"({query}) TABLESAMPLE SYSTEM ({self.sampling_percent} PERCENT)")
            
            elif db_name == "databricks":
                # Databricks supports TABLESAMPLE
                return Code(f"({query}) TABLESAMPLE ({self.sampling_percent} PERCENT)")
            
            elif db_name == "duckdb":
                # DuckDB supports TABLESAMPLE or USING SAMPLE
                return Code(f"({query}) TABLESAMPLE {self.sampling_percent}%")
            
            elif db_name in ("mssql", "sqlserver"):
                # MS SQL Server supports TABLESAMPLE
                return Code(f"({query}) TABLESAMPLE ({self.sampling_percent} PERCENT)")
            
            elif db_name == "oracle":
                # Oracle uses SAMPLE without TABLESAMPLE keyword
                return Code(f"({query}) SAMPLE({self.sampling_percent})")
            
            elif db_name in ("presto", "trino"):
                # Presto/Trino support TABLESAMPLE with BERNOULLI or SYSTEM
                method = self.sampling_method.upper()
                if method not in ("SYSTEM", "BERNOULLI"):
                    method = "BERNOULLI"  # Default to BERNOULLI for better randomness
                return Code(f"({query}) TABLESAMPLE {method}({self.sampling_percent})")
            
            elif db_name == "vertica":
                # Vertica supports TABLESAMPLE
                return Code(f"({query}) TABLESAMPLE({self.sampling_percent})")
            
            elif db_name == "redshift":
                # Redshift doesn't have TABLESAMPLE, use WHERE RANDOM() < percentage
                decimal_percent = self.sampling_percent / 100.0
                return Code(f"SELECT * FROM ({query}) WHERE RANDOM() < {decimal_percent}")
            
            else:
                # For other databases, raise an error
                raise NotImplementedError(
                    f"Database '{db_name}' does not support sampling. "
                    f"Supported databases: PostgreSQL, ClickZetta, ClickHouse, MySQL, Snowflake, "
                    f"BigQuery, Databricks, DuckDB, MS SQL Server, Oracle, Presto, Trino, Vertica, Redshift"
                )
                
        elif self.sample_size:
            # Fixed size sampling
            total_count = None
            
            if db_name in ("postgresql", "postgres"):
                # PostgreSQL: calculate percentage for TABLESAMPLE
                if self.sampling_method.upper() in ("SYSTEM", "BERNOULLI"):
                    total_count = self.count()
                    if total_count > 0:
                        # Add safety margin since TABLESAMPLE is approximate
                        percent = min(100, (self.sample_size / total_count) * 100 * 1.5)
                        return Code(f"({query}) TABLESAMPLE {self.sampling_method.upper()} ({percent})")
                
            elif db_name == "clickzetta":
                # ClickZetta: supports both percentage and row count
                if self.sample_size < 10000:  # Small sample, use row count directly
                    return Code(f"({query}) TABLESAMPLE SYSTEM ({self.sample_size} ROWS)")
                else:
                    # Large sample, calculate percentage
                    total_count = self.count()
                    if total_count > 0:
                        percent = min(100, (self.sample_size / total_count) * 100 * 1.2)
                        return Code(f"({query}) TABLESAMPLE ({percent} PERCENT)")
            
            elif db_name == "clickhouse":
                # ClickHouse supports direct row count sampling
                return Code(f"({query}) SAMPLE {self.sample_size}")
            
            elif db_name in ("mysql", "mariadb"):
                # MySQL: ORDER BY RAND() with LIMIT
                return Code(f"({query} ORDER BY RAND() LIMIT {self.sample_size})")
            
            elif db_name == "snowflake":
                # Snowflake supports SAMPLE with ROWS
                return Code(f"({query}) SAMPLE ({self.sample_size} ROWS)")
            
            elif db_name == "bigquery":
                # BigQuery: Calculate percentage
                total_count = self.count()
                if total_count > 0:
                    percent = min(100, (self.sample_size / total_count) * 100)
                    return Code(f"({query}) TABLESAMPLE SYSTEM ({percent} PERCENT)")
            
            elif db_name == "databricks":
                # Databricks supports ROWS (but uses LIMIT internally)
                return Code(f"({query}) TABLESAMPLE ({self.sample_size} ROWS)")
            
            elif db_name == "duckdb":
                # DuckDB: calculate percentage
                total_count = self.count()
                if total_count > 0:
                    percent = min(100, (self.sample_size / total_count) * 100)
                    return Code(f"({query}) TABLESAMPLE {percent}%")
            
            elif db_name in ("mssql", "sqlserver"):
                # MS SQL Server supports ROWS
                return Code(f"({query}) TABLESAMPLE ({self.sample_size} ROWS)")
            
            elif db_name == "oracle":
                # Oracle: calculate percentage
                total_count = self.count()
                if total_count > 0:
                    percent = min(100, (self.sample_size / total_count) * 100)
                    return Code(f"({query}) SAMPLE({percent})")
            
            elif db_name in ("presto", "trino"):
                # Presto/Trino: calculate percentage
                total_count = self.count()
                if total_count > 0:
                    percent = min(100, (self.sample_size / total_count) * 100 * 1.2)
                    method = self.sampling_method.upper()
                    if method not in ("SYSTEM", "BERNOULLI"):
                        method = "BERNOULLI"
                    return Code(f"({query}) TABLESAMPLE {method}({percent})")
            
            elif db_name == "vertica":
                # Vertica: calculate percentage
                total_count = self.count()
                if total_count > 0:
                    percent = min(100, (self.sample_size / total_count) * 100)
                    return Code(f"({query}) TABLESAMPLE({percent})")
            
            elif db_name == "redshift":
                # Redshift: use WHERE RANDOM() with approximate percentage
                total_count = self.count()
                if total_count > 0:
                    # Add safety margin for randomness
                    decimal_percent = min(1.0, (self.sample_size / total_count) * 1.5)
                    return Code(
                        f"SELECT * FROM ({query}) WHERE RANDOM() < {decimal_percent} "
                        f"LIMIT {self.sample_size}"
                    )
            
            # If no sampling method matched, raise an error
            raise NotImplementedError(
                f"Database '{db_name}' does not support sampling with sample_size. "
                f"All supported databases have been implemented."
            )
        
        return query
    
    def _apply_deterministic_sampling(self, query):
        """
        Apply deterministic sampling that produces the same results across different databases.
        Uses modulo operation on key columns to ensure both sides sample the same rows.
        """
        if not self.key_columns:
            raise ValueError("Deterministic sampling requires key columns to be specified")
        
        db_name = self.database.name.lower()
        
        # Calculate sampling rate
        if self.sampling_percent:
            # For percentage, use modulo to select rows
            # E.g., 10% sampling = select rows where key % 10 = 0
            modulo = int(100 / self.sampling_percent)
        elif self.sample_size:
            # For fixed size, we need to know total count
            total_count = self.count()
            if total_count <= self.sample_size:
                return query  # No sampling needed
            modulo = max(2, int(total_count / self.sample_size))
        else:
            return query  # No sampling configured
        
        # Build the deterministic WHERE clause based on key column(s)
        if len(self.key_columns) == 1:
            # Single key column - use modulo directly
            key_col = self.key_columns[0]
            
            # Different databases have different modulo syntax
            if db_name in ("postgresql", "postgres", "redshift"):
                where_clause = f"(CAST({key_col} AS BIGINT) % {modulo}) = 0"
            elif db_name == "clickzetta":
                where_clause = f"(toInt64({key_col}) % {modulo}) = 0"
            elif db_name == "clickhouse":
                where_clause = f"(toUInt64({key_col}) % {modulo}) = 0"
            elif db_name in ("mysql", "mariadb"):
                where_clause = f"(CAST({key_col} AS UNSIGNED) % {modulo}) = 0"
            elif db_name == "snowflake":
                where_clause = f"(CAST({key_col} AS NUMBER) % {modulo}) = 0"
            elif db_name == "bigquery":
                where_clause = f"(CAST({key_col} AS INT64) % {modulo}) = 0"
            elif db_name in ("databricks", "spark"):
                where_clause = f"(CAST({key_col} AS BIGINT) % {modulo}) = 0"
            elif db_name == "duckdb":
                where_clause = f"(CAST({key_col} AS BIGINT) % {modulo}) = 0"
            elif db_name in ("mssql", "sqlserver"):
                where_clause = f"(CAST({key_col} AS BIGINT) % {modulo}) = 0"
            elif db_name == "oracle":
                where_clause = f"(MOD(TO_NUMBER({key_col}), {modulo})) = 0"
            elif db_name in ("presto", "trino"):
                where_clause = f"(CAST({key_col} AS BIGINT) % {modulo}) = 0"
            elif db_name == "vertica":
                where_clause = f"(CAST({key_col} AS INT) % {modulo}) = 0"
            else:
                # Generic SQL
                where_clause = f"(CAST({key_col} AS INTEGER) % {modulo}) = 0"
                
        else:
            # Multiple key columns - use hash of concatenated keys
            # This ensures consistent sampling across composite keys
            key_concat = " || ".join([f"CAST({k} AS VARCHAR)" for k in self.key_columns])
            
            if db_name in ("postgresql", "postgres"):
                # PostgreSQL has built-in hashtext function
                where_clause = f"(ABS(hashtext({key_concat})) % {modulo}) = 0"
            elif db_name == "clickzetta":
                where_clause = f"(cityHash64({key_concat}) % {modulo}) = 0"
            elif db_name == "clickhouse":
                where_clause = f"(cityHash64({key_concat}) % {modulo}) = 0"
            elif db_name in ("mysql", "mariadb"):
                # MySQL uses CRC32 for hashing
                where_clause = f"(CRC32({key_concat}) % {modulo}) = 0"
            elif db_name == "snowflake":
                where_clause = f"(ABS(HASH({key_concat})) % {modulo}) = 0"
            elif db_name == "bigquery":
                where_clause = f"(ABS(FARM_FINGERPRINT({key_concat})) % {modulo}) = 0"
            elif db_name == "redshift":
                where_clause = f"(ABS(CHECKSUM({key_concat})) % {modulo}) = 0"
            else:
                # Fallback: sample based on first key only
                logger.warning(f"Database {db_name} doesn't support hash functions for composite keys. Using first key only.")
                key_col = self.key_columns[0]
                where_clause = f"(CAST({key_col} AS INTEGER) % {modulo}) = 0"
        
        # Apply the WHERE clause
        existing_where = self._where()
        if existing_where:
            combined_where = f"({existing_where}) AND ({where_clause})"
        else:
            combined_where = where_clause
            
        # Return a new query with the deterministic sampling condition
        return self.source_table.where(
            *self._make_key_range(), 
            *self._make_update_range(), 
            Code(combined_where)
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"

        # Fetch all the original columns, even if some were later excluded from checking.
        fetched_cols = [NormalizeAsString(this[c]) for c in self.relevant_columns]
        select = self.make_select().select(*fetched_cols)
        return self.database.query(select, List[Tuple])

    def choose_checkpoints(self, count: int) -> List[List[DbKey]]:
        "Suggests a bunch of evenly-spaced checkpoints to split by, including start, end."

        assert self.is_bounded

        # Take Nth root of count, to approximate the appropriate box size
        count = int(count ** (1 / len(self.key_columns))) or 1

        return split_compound_key_space(self.min_key, self.max_key, count)

    def segment_by_checkpoints(self, checkpoints: List[List[DbKey]]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separated by the given checkpoints"

        return [self.new_key_bounds(min_key=s, max_key=e) for s, e in create_mesh_from_points(*checkpoints)]

    def new(self, **kwargs) -> Self:
        """Creates a copy of the instance using 'replace()'"""
        return attrs.evolve(self, **kwargs)

    def new_key_bounds(self, min_key: Vector, max_key: Vector, *, key_types: Optional[Sequence[IKey]] = None) -> Self:
        if self.min_key is not None:
            assert self.min_key <= min_key, (self.min_key, min_key)
            assert self.min_key < max_key

        if self.max_key is not None:
            assert min_key < self.max_key
            assert max_key <= self.max_key

        # If asked, enforce the PKs to proper types, mainly to meta-params of the relevant side,
        # so that we do not leak e.g. casing of UUIDs from side A to side B and vice versa.
        # If not asked, keep the meta-params of the keys as is (assume them already casted).
        if key_types is not None:
            min_key = Vector(type.make_value(val) for type, val in safezip(key_types, min_key))
            max_key = Vector(type.make_value(val) for type, val in safezip(key_types, max_key))

        return attrs.evolve(self, min_key=min_key, max_key=max_key)

    @property
    def relevant_columns(self) -> List[str]:
        extras = list(self.extra_columns)

        if self.update_column and self.update_column not in extras:
            extras = [self.update_column] + extras

        return list(self.key_columns) + extras

    def count(self) -> int:
        """Count how many rows are in the segment, in one pass."""
        return self.database.query(self.make_select().select(Count()), int)

    def count_and_checksum(self) -> Tuple[int, int]:
        """Count and checksum the rows in the segment, in one pass."""

        checked_columns = [c for c in self.relevant_columns if c not in self.ignored_columns]
        cols = [NormalizeAsString(this[c]) for c in checked_columns]

        start = time.monotonic()
        q = self.make_select().select(Count(), Checksum(cols))
        count, checksum = self.database.query(q, tuple)
        duration = time.monotonic() - start
        if duration > RECOMMENDED_CHECKSUM_DURATION:
            logger.warning(
                "Checksum is taking longer than expected (%.2f). "
                "We recommend increasing --bisection-factor or decreasing --threads.",
                duration,
            )

        if count:
            assert checksum, (count, checksum)
        return count or 0, int(checksum) if count else None

    def query_key_range(self) -> Tuple[tuple, tuple]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        # Normalizes the result (needed for UUIDs) after the min/max computation
        select = self.make_select().select(
            ApplyFuncAndNormalizeAsString(this[k], f) for k in self.key_columns for f in (min_, max_)
        )
        result = tuple(self.database.query(select, tuple))

        if any(i is None for i in result):
            raise ValueError("Table appears to be empty")

        # Min/max keys are interleaved
        min_key, max_key = result[::2], result[1::2]
        assert len(min_key) == len(max_key)

        return min_key, max_key

    @property
    def is_bounded(self):
        return self.min_key is not None and self.max_key is not None

    def approximate_size(self):
        if not self.is_bounded:
            raise RuntimeError("Cannot approximate the size of an unbounded segment. Must have min_key and max_key.")
        diff = self.max_key - self.min_key
        assert all(d > 0 for d in diff)
        return int_product(diff)
