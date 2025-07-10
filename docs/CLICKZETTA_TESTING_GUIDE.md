# Clickzetta N8N 节点测试指南

## 🔧 正确的 Clickzetta 连接配置

基于测试文件 `/Users/liangmo/Documents/GitHub/data-diff-n8n/n8n/tests/test_clickzetta_connector.py`，Clickzetta 连接需要以下参数：

### 📋 必需参数

| 参数名 | 描述 | 示例值 |
|--------|------|--------|
| **Username** | Clickzetta 用户名 | `your_username` |
| **Password** | Clickzetta 密码 | `your_password` |
| **Service** | 服务名称 | `clickzetta-serverless` |
| **Instance** | 实例名称 | `your-instance-name` |
| **Workspace** | 工作空间名称 | `default` |
| **VCluster** | 虚拟集群名称 | `default` |
| **Schema** | 默认模式名称 | `default` |

## 🚀 在 N8N 中测试步骤

### 1. 访问 N8N 界面
打开浏览器访问：http://localhost:5678

### 2. 创建 Clickzetta 凭证
1. 点击右上角的设置图标
2. 选择 "Credentials"
3. 点击 "Create New"
4. 搜索并选择 "Clickzetta API"
5. 填入正确的连接参数（参考上表）
6. **注意**：Clickzetta 凭证**没有内置的连接测试**，因为它使用数据库协议而不是 HTTP API
7. 保存凭证

### 3. 创建测试工作流
1. 点击 "Create New Workflow"
2. 添加 "Clickzetta Connector" 节点
3. 选择刚创建的凭证
4. 选择操作类型：
   - **Test Connection**: 测试数据库连接（真正的连接测试在这里进行）
   - **Get Schema Info**: 获取模式信息
   - **List Tables**: 列出表格

### 4. 真正的连接测试
**重要**：Clickzetta 的连接测试是在 Clickzetta Connector 节点中进行的，而不是在凭证级别。
#### 测试连接
- 选择 "Test Connection" 操作
- 执行节点
- 检查输出是否显示连接成功

#### 获取模式信息
- 选择 "Get Schema Info" 操作
- 可选：输入特定的模式名称
- 执行节点
- 检查返回的表格和视图列表

#### 列出表格
- 选择 "List Tables" 操作
- 可选：输入特定的模式名称
- 执行节点
- 检查返回的表格列表

## 🔍 期望的输出格式

### 测试连接成功
```json
{
  "operation": "testConnection",
  "success": true,
  "data": {
    "status": "connected",
    "message": "Connection test successful",
    "connectionInfo": {
      "username": "your_username",
      "service": "clickzetta-serverless",
      "instance": "your-instance",
      "workspace": "default",
      "vcluster": "default",
      "schema": "default"
    }
  },
  "timestamp": "2025-07-08T12:00:00.000Z"
}
```

### 获取模式信息成功
```json
{
  "operation": "getSchema",
  "success": true,
  "data": {
    "schema": "default",
    "service": "clickzetta-serverless",
    "instance": "your-instance",
    "workspace": "default",
    "vcluster": "default",
    "tables": [
      {"name": "example_table_1", "type": "TABLE"},
      {"name": "example_table_2", "type": "TABLE"}
    ],
    "views": [],
    "message": "Schema information retrieved for: default"
  },
  "timestamp": "2025-07-08T12:00:00.000Z"
}
```

## ⚠️ 常见问题排查

### 连接失败
如果连接测试失败，检查以下项目：
1. **凭证参数**：确保所有必需参数都已正确填写
2. **网络连接**：确保可以访问 Clickzetta 服务
3. **用户权限**：确保用户有访问指定实例和工作空间的权限

### 参数获取
可以从以下位置获取正确的参数：
- 查看您的 Clickzetta 配置文件：`~/.clickzetta/connections.json`
- 联系您的 Clickzetta 管理员获取实例和工作空间信息

## 🔄 后续开发

当前实现返回模拟数据。在生产环境中，需要：

1. **集成 Clickzetta Python SDK**
2. **实现真实的数据库查询**
3. **添加错误处理和重试机制**
4. **支持更多操作类型**

## 📞 获取帮助

如果遇到问题：
1. 检查 N8N 节点的执行日志
2. 查看 Docker 容器日志：`docker-compose logs n8n`
3. 参考测试文件中的工作示例
