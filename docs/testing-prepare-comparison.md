# 测试 Prepare for Comparison 功能

## 前置条件

### 1. 配置数据库凭据

#### DatabaseConnector 凭据设置
1. 在 n8n 中，进入 Settings > Credentials
2. 创建新凭据：选择 "Database Connector Credentials"
3. 填写必要字段：
   ```yaml
   Database Type: postgresql (或其他)
   Host: localhost
   Port: 5432
   Database: mydb
   Username: user
   Password: pass
   Schema: public (可选)
   SSL: false
   ```

#### ClickzettaConnector 凭据设置
1. 创建新凭据：选择 "Clickzetta Api"
2. 填写所有必需字段：
   ```yaml
   Username: your_username
   Password: your_password
   Instance: your_instance
   Service: your_service
   Workspace: your_workspace
   Virtual Cluster: your_vcluster
   Schema: default (可选)
   ```

## 测试步骤

### 步骤 1：测试基本连接
1. 添加 DatabaseConnector 节点
2. 选择操作：Test Connection
3. 执行节点
4. 检查输出是否包含 `success: true`

### 步骤 2：测试 Prepare for Comparison
1. 将操作改为：Prepare for Comparison
2. 配置参数：
   - Schema Name: 留空（使用凭据中的默认值）
   - Include Sample Data: false
   - Table Filter: 留空
3. 执行节点
4. 检查输出结构

## 预期输出

### 成功的输出示例
```json
{
  "operation": "prepareComparison",
  "databaseType": "postgresql",
  "success": true,
  "connectionUrl": "postgresql://user:pass@localhost:5432/mydb",
  "connectionConfig": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "username": "user",
    "password": "pass",
    "database": "mydb",
    "schema": "public"
  },
  "tables": [
    {
      "name": "public.users",
      "value": "public.users",
      "description": "Table: users"
    }
  ],
  "comparisonReady": true,
  "comparisonConfig": {
    "source_config": {...},
    "available_tables": [...],
    "database_type": "postgresql"
  }
}
```

### 错误输出示例（凭据问题）
```json
{
  "operation": "prepareComparison",
  "databaseType": "unknown",
  "success": false,
  "error": "Could not get parameter",
  "hint": "Please ensure database credentials are properly configured",
  "errorType": "credentials",
  "credentialName": "databaseConnectorCredentials",
  "timestamp": "2025-07-12T07:15:06.259Z"
}
```

## 故障排除

### 1. "Could not get parameter" 错误
**原因**：凭据未正确配置
**解决方案**：
- 确保已创建并选择了正确的凭据
- 检查凭据中的所有必填字段是否已填写
- 尝试先运行 Test Connection 操作

### 2. "databaseType: unknown" 错误
**原因**：凭据中未设置数据库类型
**解决方案**：
- 编辑凭据，确保选择了正确的 Database Type
- 支持的类型：postgresql, mysql, clickzetta, sqlserver, oracle 等

### 3. 表列表为空
**原因**：
- 数据库中没有表
- 用户权限不足
- Schema 名称错误

**解决方案**：
- 确认数据库中存在表
- 检查数据库用户权限
- 尝试指定正确的 Schema Name

### 4. API 连接错误
**原因**：后端 API 服务未启动
**解决方案**：
- 确保 data-diff-api 容器正在运行
- 检查 docker-compose logs data-diff-api

## 测试清单

- [ ] DatabaseConnector - Test Connection 成功
- [ ] DatabaseConnector - Prepare for Comparison 成功
- [ ] ClickzettaConnector - Test Connection 成功
- [ ] ClickzettaConnector - Prepare for Comparison 成功
- [ ] 输出包含 `comparisonReady: true`
- [ ] 输出包含 `connectionConfig`
- [ ] 输出包含 `tables` 数组
- [ ] 连接到 Data Comparison 节点并自动识别配置

## 高级测试

### 1. 测试表过滤
```yaml
Table Filter: user%  # 只返回以 user 开头的表
```

### 2. 测试示例数据
```yaml
Include Sample Data: true
Sample Size: 3
```

### 3. 测试错误场景
- 故意使用错误的凭据
- 断开数据库连接
- 使用不存在的 schema

通过这些测试，可以确保 Prepare for Comparison 功能正常工作。