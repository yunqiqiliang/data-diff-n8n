#!/bin/bash
# 安全检查脚本 - 在推送前检查敏感信息

echo "执行安全检查..."
echo ""

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查函数
check_files() {
    local pattern=$1
    local description=$2
    local files=$(git ls-files | grep -E "$pattern" || true)
    
    if [ -n "$files" ]; then
        echo -e "${RED}[警告]${NC} 发现 $description:"
        echo "$files" | sed 's/^/  - /'
        return 1
    fi
    return 0
}

# 检查内容函数
check_content() {
    local pattern=$1
    local description=$2
    local exclude_pattern=$3
    
    if [ -n "$exclude_pattern" ]; then
        local matches=$(git grep -l -E "$pattern" -- ':!*.md' ':!scripts/security-check.sh' | grep -v -E "$exclude_pattern" || true)
    else
        local matches=$(git grep -l -E "$pattern" -- ':!*.md' ':!scripts/security-check.sh' || true)
    fi
    
    if [ -n "$matches" ]; then
        echo -e "${YELLOW}[提示]${NC} 发现可能包含 $description 的文件:"
        echo "$matches" | sed 's/^/  - /'
        return 1
    fi
    return 0
}

errors=0
warnings=0

# 1. 检查敏感文件
echo "1. 检查敏感文件..."
check_files "\.env(\.|$)" "环境配置文件" || ((errors++))
check_files "\.(key|pem|crt|cer|pfx|p12)$" "证书和密钥文件" || ((errors++))
check_files "\.git-credentials" "Git凭据文件" || ((errors++))
check_files "\.aws/credentials" "AWS凭据文件" || ((errors++))

# 2. 检查硬编码的密码
echo ""
echo "2. 检查硬编码的密码..."
check_content "password\s*[:=]\s*['\"][^'\"]{4,}['\"]" "硬编码密码" "\.example$|test" || ((warnings++))
check_content "api[_-]?key\s*[:=]\s*['\"][^'\"]{10,}['\"]" "API密钥" "\.example$|test" || ((warnings++))
check_content "secret\s*[:=]\s*['\"][^'\"]{10,}['\"]" "密钥" "\.example$|test" || ((warnings++))

# 3. 检查IP地址
echo ""
echo "3. 检查IP地址..."
check_content "\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b" "IP地址" "127\.0\.0\.1|0\.0\.0\.0|localhost" || ((warnings++))

# 4. 检查数据库连接字符串
echo ""
echo "4. 检查数据库连接字符串..."
check_content "mongodb://[^/]+:[^@]+@" "MongoDB连接字符串" || ((warnings++))
check_content "postgres://[^/]+:[^@]+@" "PostgreSQL连接字符串" || ((warnings++))
check_content "mysql://[^/]+:[^@]+@" "MySQL连接字符串" || ((warnings++))

# 总结
echo ""
echo "========================================="
if [ $errors -eq 0 ] && [ $warnings -eq 0 ]; then
    echo -e "${GREEN}✓ 安全检查通过！${NC}"
else
    if [ $errors -gt 0 ]; then
        echo -e "${RED}✗ 发现 $errors 个严重安全问题！${NC}"
        echo "  请在提交前解决这些问题。"
    fi
    if [ $warnings -gt 0 ]; then
        echo -e "${YELLOW}⚠ 发现 $warnings 个潜在安全警告。${NC}"
        echo "  请检查这些项目是否为测试数据或示例。"
    fi
fi
echo "========================================="

# 返回错误码
if [ $errors -gt 0 ]; then
    exit 1
else
    exit 0
fi