# Database Connector 凭证配置修复

## 问题描述
原来的 `DatabaseConnector` 节点无法保存凭证信息，因为它没有配置 N8N 的 `credentials` 属性。

## 解决方案

### 1. 创建了 `DatabaseConnectorCredentials` 凭证类
- 文件位置：`/n8n/src/credentials/DatabaseConnectorCredentials.credentials.ts`
- 支持所有数据库类型的凭证配置
- 包含敏感信息的字段（如密码）会被安全加密存储

### 2. 修改了 `DatabaseConnector` 节点
- 添加了 `credentials` 属性配置
- 简化了节点参数，移除了重复的连接信息字段
- 现在只保留操作相关的参数（操作类型、可选的 schema 覆盖）

### 3. 主要变更
- 节点现在使用 `this.getCredentials()` 获取凭证信息
- 连接配置构建逻辑更新为使用凭证对象
- 保持向后兼容性，所有数据库类型仍然支持

## 使用方式

### 步骤 1：创建凭证
1. 在 N8N 中打开 "Credentials" 页面
2. 点击 "Create New Credential"
3. 选择 "Database Connector Credentials"
4. 配置数据库连接信息：
   - 选择数据库类型
   - 填写主机、端口、用户名、密码等信息
   - 保存凭证

### 步骤 2：使用节点
1. 在工作流中添加 "Database Connector" 节点
2. 选择之前创建的凭证
3. 选择操作类型（测试连接、获取模式信息、列出表）
4. 可选：覆盖默认的 schema 名称

## 支持的数据库类型
- PostgreSQL
- MySQL
- ClickHouse
- Clickzetta
- Snowflake
- BigQuery
- Redshift
- Oracle
- SQL Server
- DuckDB
- Databricks
- Trino
- Presto
- Vertica

## 安全性改进
- 敏感信息（密码、令牌等）现在通过 N8N 的凭证系统安全存储
- 凭证信息加密存储，不会出现在工作流定义中
- 支持凭证的重复使用和统一管理

## 文件变更列表
- `新增`: `/n8n/src/credentials/DatabaseConnectorCredentials.credentials.ts`
- `修改`: `/n8n/src/nodes/DatabaseConnector/DatabaseConnector.node.ts`
- `修改`: `/n8n/src/index.ts`
