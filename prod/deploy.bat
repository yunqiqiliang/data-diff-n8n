@echo off
REM Data-Diff N8N 生产部署脚本 (Windows)

setlocal enabledelayedexpansion

REM 设置版本号
set VERSION=latest

REM 显示帮助
if "%1"=="help" goto :show_help
if "%1"=="-h" goto :show_help
if "%1"=="--help" goto :show_help

REM 检查 Docker
:check_docker
echo [INFO] 检查 Docker 环境...
docker version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker 未安装或未运行！请先安装并启动 Docker Desktop
    exit /b 1
)
echo [SUCCESS] Docker 环境检查通过

REM 主命令处理
if "%1"=="setup" goto :setup
if "%1"=="deploy" goto :deploy
if "%1"=="start" goto :start
if "%1"=="stop" goto :stop
if "%1"=="restart" goto :restart
if "%1"=="status" goto :status
if "%1"=="logs" goto :logs
if "%1"=="backup" goto :backup

echo [ERROR] 未知命令: %1
goto :show_help

:show_help
echo.
echo Data-Diff N8N 生产部署脚本
echo.
echo 用法: deploy.bat [命令] [选项]
echo.
echo 命令:
echo   setup     初始化部署环境
echo   deploy    部署应用
echo   start     启动服务
echo   stop      停止服务
echo   restart   重启服务
echo   status    查看服务状态
echo   logs      查看日志
echo   backup    备份数据
echo.
echo 示例:
echo   deploy.bat setup     - 初始化环境
echo   deploy.bat deploy    - 部署应用
echo   deploy.bat logs api  - 查看 API 日志
echo.
goto :end

:setup
echo [INFO] 初始化部署环境...

REM 创建目录
mkdir nginx\ssl 2>nul
mkdir nginx\static 2>nul
mkdir monitoring\grafana-config 2>nul
mkdir monitoring\dashboards 2>nul
mkdir init 2>nul
mkdir n8n-nodes 2>nul

REM 复制配置文件
if not exist .env (
    if exist .env.example.prod (
        copy .env.example.prod .env
        echo [WARNING] 已创建 .env 文件，请编辑配置后再继续部署
        echo 使用记事本编辑: notepad .env
        goto :end
    ) else (
        echo [ERROR] .env.example.prod 文件不存在
        exit /b 1
    )
)

REM 复制文件
if exist monitoring\grafana xcopy /E /Y monitoring\grafana\* monitoring\grafana-config\ >nul 2>&1
if exist monitoring\grafana-dashboards copy monitoring\grafana-dashboards\*.json monitoring\dashboards\ >nul 2>&1
if exist nginx\index.html copy nginx\index.html nginx\static\ >nul 2>&1

echo [SUCCESS] 环境初始化完成
goto :end

:deploy
call :check_docker

if not exist .env (
    echo [ERROR] 未找到 .env 文件，请先运行 setup 命令
    exit /b 1
)

echo [INFO] 开始部署 Data-Diff N8N...
echo [INFO] 拉取 Docker 镜像...
docker-compose -f docker-compose.prod.yml pull

echo [INFO] 启动服务...
docker-compose -f docker-compose.prod.yml up -d

echo [INFO] 等待服务启动...
timeout /t 30 /nobreak >nul

call :check_services
echo [SUCCESS] 部署完成！
call :show_access_info
goto :end

:start
echo [INFO] 启动服务...
docker-compose -f docker-compose.prod.yml up -d
echo [SUCCESS] 服务已启动
goto :end

:stop
echo [INFO] 停止服务...
docker-compose -f docker-compose.prod.yml down
echo [SUCCESS] 服务已停止
goto :end

:restart
call :stop
call :start
goto :end

:status
call :check_services
goto :end

:logs
if "%2"=="" (
    docker-compose -f docker-compose.prod.yml logs -f --tail=100
) else (
    docker-compose -f docker-compose.prod.yml logs -f --tail=100 %2
)
goto :end

:backup
set timestamp=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set timestamp=%timestamp: =0%
set backup_dir=backups\backup_%timestamp%

echo [INFO] 开始备份数据到 %backup_dir%...
mkdir %backup_dir% 2>nul

echo [INFO] 备份配置文件...
copy .env %backup_dir%\ >nul
copy docker-compose.prod.yml %backup_dir%\ >nul

echo [SUCCESS] 备份完成！备份文件保存在: %backup_dir%
goto :end

:check_services
echo [INFO] 检查服务状态...
docker-compose -f docker-compose.prod.yml ps
goto :eof

:show_access_info
echo.
echo [INFO] 服务访问地址：
echo   - 主页: http://localhost
echo   - Data-Diff API: http://localhost:8000
echo   - N8N 工作流: http://localhost:5678
echo   - Grafana 监控: http://localhost:3000
echo   - Prometheus: http://localhost:9090
echo.
echo [INFO] 默认登录信息：
echo   - 查看 .env 文件获取用户名和密码
echo.
goto :eof

:end
endlocal