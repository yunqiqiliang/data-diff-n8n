from typing import Any, Dict, Sequence
import logging

import attrs

from data_diff.abcs.database_types import (
    Integer,
    Float,
    Decimal,
    Timestamp,
    Text,
    TemporalType,
    NumericType,
    DbPath,
    ColType,
    UnknownColType,
    Boolean,
    Date,
)
from data_diff.databases.base import (
    MD5_HEXDIGITS,
    CHECKSUM_HEXDIGITS,
    BaseDialect,
    ThreadedDatabase,
    import_helper,
    parse_table_name,
)
from data_diff.schema import RawColumnInfo


@import_helper(text="You can install it using 'pip install clickzetta-connector'")
def import_clickzetta():
    import clickzetta

    return clickzetta


@attrs.define(frozen=False)
class Dialect(BaseDialect):
    name = "Clickzetta"
    ROUNDS_ON_PREC_LOSS = True
    TYPE_CLASSES = {
        # Numbers
        "INT": Integer,
        "SMALLINT": Integer,
        "TINYINT": Integer,
        "BIGINT": Integer,
        "FLOAT": Float,
        "DOUBLE": Float,
        "DECIMAL": Decimal,
        # Date
        "DATE": Date,
        # Timestamps
        "TIMESTAMP": Timestamp,
        # Text
        "STRING": Text,
        "CHAR": Text,
        "VARCHAR": Text,
        # Boolean
        "BOOLEAN": Boolean,
    }

    def md5_as_int(self, s: str) -> str:
        return f"cast(conv(substr(md5({s}), {1 + MD5_HEXDIGITS - CHECKSUM_HEXDIGITS}), 16, 10) as decimal(38, 0))"

    def md5_as_hex(self, s: str) -> str:
        return f"md5({s})"

    def normalize_timestamp(self, value: str, coltype: TemporalType) -> str:
        precision_format = "S" * coltype.precision + "0" * (6 - coltype.precision)
        return f"date_format({value}, 'yyyy-MM-dd HH:mm:ss.{precision_format}')"

    def normalize_number(self, value: str, coltype: NumericType) -> str:
        return self.to_string(f"cast({value} as decimal(38, {coltype.precision}))")

    def normalize_boolean(self, value: str, _coltype: Boolean) -> str:
        return self.to_string(f"cast ({value} as int)")

    def quote(self, s: str):
        return f"`{s}`"

    def to_string(self, s: str) -> str:
        return f"cast({s} as string)"

    def _convert_db_precision_to_digits(self, p: int) -> int:
        return max(super()._convert_db_precision_to_digits(p) - 2, 0)

    def set_timezone_to_utc(self) -> str:
        raise NotImplementedError("Clickzetta does not support timezones")

    def parse_table_name(self, name: str) -> DbPath:
        path = parse_table_name(name)
        return tuple(i for i in path if i is not None)


@attrs.define(frozen=False, init=False, kw_only=True)
class Clickzetta(ThreadedDatabase):
    dialect = Dialect()
    CONNECT_URI_HELP = "clickzetta://<username>:<pwd>@<instance>.<service>/<workspace>?schema=<schema>&virtualcluster=<virtualcluster>"
    CONNECT_URI_PARAMS = ["workspace"]
    CONNECT_URI_KWPARAMS = []

    _args: Dict[str, Any]
    workspace: str

    def __init__(self, *, thread_count, **kw):
        super().__init__(thread_count=thread_count)
        logging.getLogger("clickzetta").setLevel(logging.WARNING)

        self._args = kw

        # 如果有host参数，需要分解为instance和service
        if 'host' in kw and '.' in kw['host']:
            host_parts = kw['host'].split('.', 1)
            self._args.setdefault('instance', host_parts[0])
            self._args.setdefault('service', host_parts[1])
        else:
            # 确保必要字段存在
            self._args.setdefault('instance', '')
            self._args.setdefault('service', 'uat-api.clickzetta.com')

        self._args.setdefault('workspace', 'default')
        self._args.setdefault('virtualcluster', 'default_ap')
        self.default_schema = kw.get("schema", "public")
        self.workspace = kw.get("workspace", "default")

    def create_connection(self):
        clickzetta = import_clickzetta()

        try:
            # 添加调试日志
            logging.error(f"DEBUG: Clickzetta connection args: {self._args}")
            logging.error(f"DEBUG: username={self._args.get('username', self._args.get('user'))}")
            logging.error(f"DEBUG: password={'***' if self._args.get('password') else 'None'}")
            logging.error(f"DEBUG: instance={self._args.get('instance', '')}")
            logging.error(f"DEBUG: service={self._args.get('service', 'uat-api.clickzetta.com')}")
            logging.error(f"DEBUG: workspace={self._args.get('workspace', 'default')}")
            logging.error(f"DEBUG: virtualcluster={self._args.get('virtualcluster', 'default_ap')}")
            logging.error(f"DEBUG: schema={self.default_schema}")

            return clickzetta.connect(
                username=self._args.get("username", self._args.get("user")),
                password=self._args.get("password", ""),
                instance=self._args.get("instance", ""),
                service=self._args.get("service", "uat-api.clickzetta.com"),
                workspace=self._args.get("workspace", "default"),
                vcluster=self._args.get("virtualcluster", "default_ap"),
                schema=self.default_schema,
            )
        except Exception as e:
            raise ConnectionError(*e.args) from e

    def query_table_schema(self, path: DbPath) -> Dict[str, tuple]:

        conn = self.create_connection()

        workspace, schema, table = self._normalize_table_path(path)
        with conn.cursor() as cursor:
            # 注意：Clickzetta 的 SHOW COLUMNS 语法不需要 schema 前缀
            cursor.execute(f"SHOW COLUMNS IN {table}")
            try:
                rows = cursor.fetchall()
            finally:
                conn.close()
            if not rows:
                raise RuntimeError(f"{self.name}: Table '{'.'.join(path)}' does not exist, or has no columns")

            # SHOW COLUMNS 返回格式: (schema, table, column_name, column_type, comment)
            # 我们需要 column_name 和 column_type
            d = {r[2]: (r[2], r[3].strip().split(' ')[0].upper(), None, None, None) for r in rows}
            assert len(d) == len(rows)
            return d

    def _process_table_schema(
            self, path: DbPath, raw_schema: Dict[str, tuple], filter_columns: Sequence[str], where: str = None
    ):
        accept = {i.lower() for i in filter_columns}
        rows = [row for name, row in raw_schema.items() if name.lower() in accept]

        resulted_rows = []
        for row in rows:
            row_type = "DECIMAL" if row[1].startswith("DECIMAL") else row[1]
            type_cls = self.dialect.TYPE_CLASSES.get(row_type, UnknownColType)

            if issubclass(type_cls, Integer):
                raw_info = RawColumnInfo(
                    column_name=row[0],
                    data_type=row_type,
                    datetime_precision=None,
                    numeric_precision=None,
                    numeric_scale=0
                )

            elif issubclass(type_cls, Decimal):
                items = row[1][8:].rstrip(")").split(",")
                numeric_precision, numeric_scale = int(items[0]), int(items[1])
                raw_info = RawColumnInfo(
                    column_name=row[0],
                    data_type=row_type,
                    datetime_precision=None,
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale
                )

            else:
                raw_info = RawColumnInfo(
                    column_name=row[0],
                    data_type=row_type,
                    datetime_precision=None,
                    numeric_precision=None,
                    numeric_scale=None
                )

            resulted_rows.append(raw_info)

        col_dict: Dict[str, ColType] = {info.column_name: self.dialect.parse_type(path, info) for info in resulted_rows}

        self._refine_coltypes(path, col_dict, where)
        return col_dict

    @property
    def is_autocommit(self) -> bool:
        return True

    def _normalize_table_path(self, path: DbPath) -> DbPath:
        if len(path) == 1:
            return self.workspace, self.default_schema, path[0]
        elif len(path) == 2:
            return self.workspace, path[0], path[1]
        elif len(path) == 3:
            return path

        raise ValueError(
            f"{self.name}: Bad table path for {self}: '{'.'.join(path)}'. Expected format: table, schema.table, or workspace.schema.table"
        )
