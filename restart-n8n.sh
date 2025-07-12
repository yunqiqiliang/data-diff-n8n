#!/bin/bash

echo "重启 n8n 容器以应用新的配置..."

# 停止并重启 n8n 容器
docker-compose -f docker-compose.dev.yml stop n8n
docker-compose -f docker-compose.dev.yml up -d n8n

echo "等待 n8n 启动..."
sleep 10

# 检查容器状态
echo -e "\nn8n 容器状态："
docker-compose -f docker-compose.dev.yml ps n8n

# 显示容器日志的最后几行
echo -e "\nn8n 最新日志："
docker-compose -f docker-compose.dev.yml logs --tail=20 n8n

echo -e "\n✅ n8n 已重启并应用新配置"
echo "访问地址: http://localhost:5678"
echo "用户名: admin"
echo "密码: admin123"