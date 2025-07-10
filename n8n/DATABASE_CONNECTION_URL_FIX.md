# 数据库连接 URL 格式修正

## 概述
根据 `data-diff` 库的实际连接格式要求，修正了 N8N 节点中各种数据库的连接 URL 生成逻辑。

## 修复的问题
1. **Clickzetta 连接 URL 格式错误** - 原格式不符合 data-diff 期望的格式
2. **缺少 Clickzetta 在连接注册表中** - `DATABASE_BY_SCHEME` 中没有注册 clickzetta
3. **云数据库连接字段不完整** - Snowflake、BigQuery、Databricks 缺少必要字段
4. **特殊数据库格式不正确** - DuckDB、Trino、Presto 等格式需要调整

## 修正的连接 URL 格式

### 1. Clickzetta
```
修正前: clickzetta://user:pass@instance/service?workspace=ws&vcluster=vc&schema=public
修正后: clickzetta://user:pass@instance.service/workspace?virtualcluster=vc&schema=public
```
**参考**: `data_diff/databases/clickzetta.py` 中的 `CONNECT_URI_HELP`

### 2. PostgreSQL
```
格式: postgresql://user:pass@host:port/database?sslmode=require
```
**参考**: `data_diff/databases/postgresql.py`

### 3. MySQL
```
格式: mysql://user:pass@host:port/database?ssl=true
```
**参考**: `data_diff/databases/mysql.py`

### 4. Snowflake
```
修正前: snowflake://user:pass@account.snowflakecomputing.com/?warehouse=warehouse&database=database&schema=schema
修正后: snowflake://user:pass@account/database/schema?warehouse=warehouse
```
**参考**: `data_diff/databases/snowflake.py` - `snowflake://<user>:<password>@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>`

### 5. BigQuery
```
修正前: bigquery://user:pass@project/dataset
修正后: bigquery://project/dataset
```
**参考**: `data_diff/databases/bigquery.py` - `bigquery://<project>/<dataset>`

### 6. Databricks
```
修正前: databricks://user:pass@host/database
修正后: databricks://:access_token@server_hostname/http_path
```
**参考**: `data_diff/databases/databricks.py` - `databricks://:<access_token>@<server_hostname>/<http_path>`

### 7. DuckDB
```
修正前: duckdb://database
修正后: duckdb://dbname@filepath
```
**参考**: `data_diff/databases/duckdb.py` - `duckdb://<dbname>@<filepath>`

### 8. Trino/Presto
```
修正前: trino://user:pass@host:port/catalog/schema
修正后: trino://user@host:port/catalog/schema
```
**参考**: `data_diff/databases/trino.py` - `trino://<user>@<host>/<catalog>/<schema>`

## 代码修改

### 1. 修正 `data_diff/databases/_connect.py`
```python
DATABASE_BY_SCHEME = {
    # ... 其他数据库 ...
    "clickzetta": Clickzetta,  # 新增这一行
}
```

### 2. 修正 `DatabaseConnector.node.ts` 的 `buildConnectionUrl` 方法
- 更新了所有数据库类型的 URL 生成逻辑
- 确保格式符合 data-diff 的期望

### 3. 修正 `ClickzettaConnector.node.ts` 的 `buildConnectionUrl` 方法
- 使用正确的 Clickzetta URL 格式

### 4. 增强 `DatabaseConnectorCredentials.credentials.ts`
- 为 Snowflake 添加了 `account` 和 `warehouse` 字段
- 为 BigQuery 添加了 `project` 和 `dataset` 字段
- 为 Databricks 添加了 `server_hostname`、`http_path` 和 `access_token` 字段
- 为 DuckDB 添加了 `filepath` 字段

### 5. 更新 `buildConnectionConfig` 方法
- 为每种数据库类型提供正确的配置字段映射

## 新增凭证字段

### Snowflake 凭证
- `account` - Snowflake 账户标识符
- `warehouse` - Snowflake 仓库名称

### BigQuery 凭证
- `project` - Google Cloud 项目 ID
- `dataset` - BigQuery 数据集名称

### Databricks 凭证
- `server_hostname` - Databricks 服务器主机名
- `http_path` - Databricks HTTP 路径
- `access_token` - Databricks 访问令牌

### DuckDB 凭证
- `filepath` - DuckDB 数据库文件路径（默认 `:memory:`）

## 测试验证

### 连接测试流程
1. 在 N8N 中创建对应数据库类型的凭证
2. 配置 DatabaseConnector 节点并执行 Test Connection
3. 检查输出的 `connectionUrl` 是否符合 data-diff 格式
4. 验证 DataComparison 节点能够正确使用该连接 URL

### 预期输出示例
```json
{
  "operation": "testConnection",
  "databaseType": "clickzetta",
  "success": true,
  "connectionUrl": "clickzetta://user:pass@instance.service/workspace?virtualcluster=vc&schema=public",
  "connectionConfig": { /* 配置对象 */ },
  "data": { /* API 响应 */ },
  "timestamp": "2025-07-09T..."
}
```

## 向后兼容性
- 所有现有连接配置继续有效
- 新字段为可选，有默认值
- 不影响现有工作流的运行

## 相关文件
- ✅ `data_diff/databases/_connect.py` - 添加 clickzetta 注册
- ✅ `n8n/src/nodes/DatabaseConnector/DatabaseConnector.node.ts` - 修正连接 URL 生成
- ✅ `n8n/src/nodes/ClickzettaConnector/ClickzettaConnector.node.ts` - 修正 Clickzetta URL
- ✅ `n8n/src/credentials/DatabaseConnectorCredentials.credentials.ts` - 增强凭证字段
