# 🔧 Clickzetta 连接问题修复说明

## 🐛 问题描述
之前在 N8N 中测试 Clickzetta 凭证时出现错误：
```
getaddrinfo ENOTFOUND {{$credentials.service}}.clickzetta.com
```

## ✅ 问题根源
**错误配置**：我们在 Clickzetta 凭证中添加了错误的 HTTP API 测试配置，试图通过 HTTP 连接到 `{service}.clickzetta.com`。

**正确方式**：Clickzetta 使用数据库协议连接（就像您的测试文件 `test_clickzetta_connector.py` 中展示的），而不是 HTTP API。

## 🔨 修复内容

### 1. 移除错误的 HTTP 测试配置
从 `ClickzettaApi.credentials.ts` 中移除了：
- `authenticate` 配置
- `test` 配置（HTTP 测试）
- 相关的导入

### 2. 澄清测试方法
- **凭证级别**：只存储连接参数，不进行连接测试
- **节点级别**：真正的连接测试在 "Clickzetta Connector" 节点中进行

## 🚀 现在如何测试

### 步骤1：创建凭证
1. 在 N8N 中创建 "Clickzetta API" 凭证
2. 填入您的连接参数（参考 `~/.clickzetta/connections.json` 中的 `uat` 配置）
3. 直接保存（无需测试连接）

### 步骤2：测试连接
1. 创建工作流，添加 "Clickzetta Connector" 节点
2. 选择刚创建的凭证
3. 选择 "Test Connection" 操作
4. 执行节点进行真正的数据库连接测试

## 📚 参考实现
正确的连接方法参考：
```python
# 来自 test_clickzetta_connector.py
conn = clickzetta.connect(
    username=conn_config['username'],
    password=conn_config['password'],
    service=conn_config['service'],
    instance=conn_config['instance'],
    workspace=conn_config['workspace'],
    vcluster=conn_config['vcluster'],
    schema=conn_config['schema']
)
```

## ✅ 修复状态
- ✅ 移除错误的 HTTP 测试配置
- ✅ 重新构建节点
- ✅ 重启 N8N 服务
- ✅ 更新测试指南

现在您可以正常创建 Clickzetta 凭证，不会再出现域名解析错误！
