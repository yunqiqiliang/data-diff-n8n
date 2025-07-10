# PostgreSQL 数据库类型修复

## 问题描述
之前在 N8N 凭据配置中，PostgreSQL 数据库类型被设置为 `postgresql`，但后端 API 期望的是 `postgres`。这导致了类型不匹配的问题。

## 修复内容

### 1. 更新凭据配置
- 将 PostgreSQL 选项的值从 `postgresql` 改为 `postgres`
- 保持显示名称为 "PostgreSQL"（用户看到的名称）

### 2. 更新字段显示条件
更新了以下字段的 `databaseType` 条件，将 `postgresql` 改为 `postgres`：
- Host (主机)
- Port (端口)
- Database (数据库名)
- Username (用户名)
- Password (密码)
- SSL (SSL 连接选项)

### 3. 更新后端 API
- 在 `n8n/api/main.py` 中统一使用 `postgres` 作为数据库类型判断条件

## 连接字符串格式
虽然数据库类型标识符使用 `postgres`，但连接字符串仍然使用标准的 `postgresql://` scheme：
```
postgresql://username:password@host:port/database
```

## 测试验证
- 构建成功，无 TypeScript 错误
- 数据库类型标识符与后端期望一致
- 连接字符串格式符合标准

## 文件更改
- `n8n/src/credentials/DatabaseConnectorCredentials.credentials.ts`
- `n8n/src/nodes/DatabaseConnector/DatabaseConnector.node.ts`
- `n8n/api/main.py`
