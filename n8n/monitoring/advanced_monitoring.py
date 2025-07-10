"""
高级监控和告警系统
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import requests
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class AlertManager:
    """告警管理器"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.alert_rules = {}
        self.notification_channels = {}
        self.alert_history = []
        self.executor = ThreadPoolExecutor(max_workers=5)

    async def initialize(self):
        """初始化告警管理器"""
        self.load_alert_rules()
        self.load_notification_channels()
        logger.info("Alert manager initialized")

    def load_alert_rules(self):
        """加载告警规则"""
        self.alert_rules = {
            "comparison_failure": {
                "name": "比对任务失败",
                "condition": "error_count > 0",
                "severity": "high",
                "cooldown": 300,  # 5分钟冷却期
                "channels": ["email", "slack"]
            },
            "high_diff_count": {
                "name": "差异数量过高",
                "condition": "diff_count > 1000",
                "severity": "medium",
                "cooldown": 600,  # 10分钟冷却期
                "channels": ["slack"]
            },
            "long_execution_time": {
                "name": "执行时间过长",
                "condition": "execution_time > 3600",
                "severity": "low",
                "cooldown": 1800,  # 30分钟冷却期
                "channels": ["email"]
            },
            "system_health": {
                "name": "系统健康检查失败",
                "condition": "health_status != 'healthy'",
                "severity": "critical",
                "cooldown": 60,  # 1分钟冷却期
                "channels": ["email", "slack", "webhook"]
            }
        }

    def load_notification_channels(self):
        """加载通知渠道配置"""
        self.notification_channels = {
            "email": {
                "enabled": True,
                "smtp_server": "smtp.example.com",
                "smtp_port": 587,
                "username": "alerts@example.com",
                "password": "password",
                "recipients": ["admin@example.com"]
            },
            "slack": {
                "enabled": True,
                "webhook_url": "https://hooks.slack.com/services/...",
                "channel": "#data-alerts",
                "bot_token": "xoxb-..."
            },
            "webhook": {
                "enabled": True,
                "url": "https://api.example.com/alerts",
                "headers": {"Authorization": "Bearer token"}
            }
        }

    async def check_alerts(self, metrics: Dict[str, Any]):
        """检查是否触发告警"""
        for rule_name, rule in self.alert_rules.items():
            if self.should_trigger_alert(rule, metrics):
                await self.trigger_alert(rule_name, rule, metrics)

    def should_trigger_alert(self, rule: Dict[str, Any], metrics: Dict[str, Any]) -> bool:
        """判断是否应该触发告警"""
        try:
            # 检查冷却期
            if self.is_in_cooldown(rule["name"]):
                return False

            # 评估告警条件
            condition = rule["condition"]

            # 简单的条件评估（实际应用中可以使用更复杂的表达式引擎）
            if "error_count > 0" in condition:
                return metrics.get("error_count", 0) > 0
            elif "diff_count > 1000" in condition:
                return metrics.get("diff_count", 0) > 1000
            elif "execution_time > 3600" in condition:
                return metrics.get("execution_time", 0) > 3600
            elif "health_status != 'healthy'" in condition:
                return metrics.get("health_status") != "healthy"

            return False

        except Exception as e:
            logger.error(f"Error evaluating alert condition: {e}")
            return False

    def is_in_cooldown(self, alert_name: str) -> bool:
        """检查告警是否在冷却期内"""
        for alert in self.alert_history:
            if (alert["name"] == alert_name and
                datetime.utcnow() - alert["timestamp"] < timedelta(seconds=alert["cooldown"])):
                return True
        return False

    async def trigger_alert(self, rule_name: str, rule: Dict[str, Any], metrics: Dict[str, Any]):
        """触发告警"""
        alert_data = {
            "rule_name": rule_name,
            "name": rule["name"],
            "severity": rule["severity"],
            "timestamp": datetime.utcnow(),
            "metrics": metrics,
            "cooldown": rule["cooldown"]
        }

        # 记录告警历史
        self.alert_history.append(alert_data)

        # 发送通知
        for channel in rule["channels"]:
            if channel in self.notification_channels and self.notification_channels[channel]["enabled"]:
                asyncio.create_task(self.send_notification(channel, alert_data))

        logger.info(f"Alert triggered: {rule['name']} (severity: {rule['severity']})")

    async def send_notification(self, channel: str, alert_data: Dict[str, Any]):
        """发送通知"""
        try:
            if channel == "email":
                await self.send_email_notification(alert_data)
            elif channel == "slack":
                await self.send_slack_notification(alert_data)
            elif channel == "webhook":
                await self.send_webhook_notification(alert_data)

        except Exception as e:
            logger.error(f"Failed to send {channel} notification: {e}")

    async def send_email_notification(self, alert_data: Dict[str, Any]):
        """发送邮件通知"""
        config = self.notification_channels["email"]

        def send_email():
            try:
                msg = MimeMultipart()
                msg['From'] = config["username"]
                msg['To'] = ", ".join(config["recipients"])
                msg['Subject'] = f"[{alert_data['severity'].upper()}] {alert_data['name']}"

                body = f"""
                告警名称: {alert_data['name']}
                严重程度: {alert_data['severity']}
                触发时间: {alert_data['timestamp']}

                详细信息:
                {json.dumps(alert_data['metrics'], indent=2, ensure_ascii=False)}
                """

                msg.attach(MimeText(body, 'plain', 'utf-8'))

                server = smtplib.SMTP(config["smtp_server"], config["smtp_port"])
                server.starttls()
                server.login(config["username"], config["password"])
                server.send_message(msg)
                server.quit()

                logger.info("Email notification sent successfully")

            except Exception as e:
                logger.error(f"Failed to send email: {e}")

        # 在线程池中执行邮件发送
        self.executor.submit(send_email)

    async def send_slack_notification(self, alert_data: Dict[str, Any]):
        """发送 Slack 通知"""
        config = self.notification_channels["slack"]

        color_map = {
            "low": "#36a64f",
            "medium": "#ff9500",
            "high": "#ff0000",
            "critical": "#8b0000"
        }

        payload = {
            "channel": config["channel"],
            "attachments": [
                {
                    "color": color_map.get(alert_data["severity"], "#ff0000"),
                    "title": f"🚨 {alert_data['name']}",
                    "fields": [
                        {
                            "title": "严重程度",
                            "value": alert_data["severity"],
                            "short": True
                        },
                        {
                            "title": "触发时间",
                            "value": alert_data["timestamp"].isoformat(),
                            "short": True
                        }
                    ],
                    "text": f"```\n{json.dumps(alert_data['metrics'], indent=2, ensure_ascii=False)}\n```"
                }
            ]
        }

        try:
            response = requests.post(config["webhook_url"], json=payload, timeout=10)
            response.raise_for_status()
            logger.info("Slack notification sent successfully")

        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")

    async def send_webhook_notification(self, alert_data: Dict[str, Any]):
        """发送 Webhook 通知"""
        config = self.notification_channels["webhook"]

        payload = {
            "alert": {
                "name": alert_data["name"],
                "severity": alert_data["severity"],
                "timestamp": alert_data["timestamp"].isoformat(),
                "metrics": alert_data["metrics"]
            }
        }

        try:
            response = requests.post(
                config["url"],
                json=payload,
                headers=config.get("headers", {}),
                timeout=10
            )
            response.raise_for_status()
            logger.info("Webhook notification sent successfully")

        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")


class PerformanceMonitor:
    """性能监控器"""

    def __init__(self):
        self.metrics_history = []
        self.db_path = "monitoring.db"
        self.init_database()

    def init_database(self):
        """初始化监控数据库"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    metric_type TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    value REAL NOT NULL,
                    metadata TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS comparisons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    comparison_id TEXT UNIQUE NOT NULL,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME,
                    status TEXT NOT NULL,
                    source_table TEXT,
                    target_table TEXT,
                    rows_compared INTEGER,
                    differences_found INTEGER,
                    execution_time REAL,
                    error_message TEXT
                )
            """)

            conn.commit()
            conn.close()

            logger.info("Monitoring database initialized")

        except Exception as e:
            logger.error(f"Failed to initialize monitoring database: {e}")

    def record_metric(self, metric_type: str, metric_name: str, value: float, metadata: Optional[Dict] = None):
        """记录指标"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO metrics (metric_type, metric_name, value, metadata)
                VALUES (?, ?, ?, ?)
            """, (metric_type, metric_name, value, json.dumps(metadata) if metadata else None))

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"Failed to record metric: {e}")

    def record_comparison(self, comparison_data: Dict[str, Any]):
        """记录比对任务信息"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT OR REPLACE INTO comparisons
                (comparison_id, start_time, end_time, status, source_table, target_table,
                 rows_compared, differences_found, execution_time, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comparison_data.get("comparison_id"),
                comparison_data.get("start_time"),
                comparison_data.get("end_time"),
                comparison_data.get("status"),
                comparison_data.get("source_table"),
                comparison_data.get("target_table"),
                comparison_data.get("rows_compared"),
                comparison_data.get("differences_found"),
                comparison_data.get("execution_time"),
                comparison_data.get("error_message")
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"Failed to record comparison: {e}")

    def get_metrics_summary(self, hours: int = 24) -> Dict[str, Any]:
        """获取指标摘要"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 获取最近的比对统计
            cursor.execute("""
                SELECT
                    COUNT(*) as total_comparisons,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_comparisons,
                    AVG(execution_time) as avg_execution_time,
                    SUM(differences_found) as total_differences
                FROM comparisons
                WHERE start_time >= datetime('now', '-{} hours')
            """.format(hours))

            comparison_stats = cursor.fetchone()

            # 获取性能指标
            cursor.execute("""
                SELECT metric_name, AVG(value) as avg_value
                FROM metrics
                WHERE timestamp >= datetime('now', '-{} hours')
                AND metric_type = 'performance'
                GROUP BY metric_name
            """.format(hours))

            performance_metrics = dict(cursor.fetchall())

            conn.close()

            return {
                "period_hours": hours,
                "comparisons": {
                    "total": comparison_stats[0] or 0,
                    "successful": comparison_stats[1] or 0,
                    "success_rate": (comparison_stats[1] / comparison_stats[0] * 100) if comparison_stats[0] else 0,
                    "avg_execution_time": comparison_stats[2] or 0,
                    "total_differences": comparison_stats[3] or 0
                },
                "performance": performance_metrics,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to get metrics summary: {e}")
            return {}

    def get_health_status(self) -> Dict[str, Any]:
        """获取系统健康状态"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 检查最近1小时的错误率
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status != 'completed' THEN 1 ELSE 0 END) as errors
                FROM comparisons
                WHERE start_time >= datetime('now', '-1 hour')
            """)

            recent_stats = cursor.fetchone()
            error_rate = (recent_stats[1] / recent_stats[0] * 100) if recent_stats[0] else 0

            # 确定健康状态
            if error_rate == 0:
                status = "healthy"
            elif error_rate < 10:
                status = "warning"
            else:
                status = "unhealthy"

            conn.close()

            return {
                "status": status,
                "error_rate": error_rate,
                "recent_comparisons": recent_stats[0],
                "recent_errors": recent_stats[1],
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to get health status: {e}")
            return {"status": "unknown", "error": str(e)}


class MonitoringService:
    """监控服务主类"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.alert_manager = AlertManager(config_manager)
        self.performance_monitor = PerformanceMonitor()
        self.monitoring_active = False
        self.monitoring_task = None

    async def initialize(self):
        """初始化监控服务"""
        await self.alert_manager.initialize()
        self.monitoring_active = True

        # 启动监控循环
        self.monitoring_task = asyncio.create_task(self.monitoring_loop())

        logger.info("Monitoring service initialized")

    async def shutdown(self):
        """关闭监控服务"""
        self.monitoring_active = False

        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("Monitoring service shutdown")

    async def monitoring_loop(self):
        """监控循环"""
        while self.monitoring_active:
            try:
                # 收集指标
                metrics = await self.collect_metrics()

                # 检查告警
                await self.alert_manager.check_alerts(metrics)

                # 等待下一次检查
                await asyncio.sleep(60)  # 每分钟检查一次

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)

    async def collect_metrics(self) -> Dict[str, Any]:
        """收集系统指标"""
        try:
            # 获取系统健康状态
            health_status = self.performance_monitor.get_health_status()

            # 获取指标摘要
            metrics_summary = self.performance_monitor.get_metrics_summary(hours=1)

            return {
                "health_status": health_status["status"],
                "error_count": health_status["recent_errors"],
                "error_rate": health_status["error_rate"],
                "total_comparisons": metrics_summary.get("comparisons", {}).get("total", 0),
                "successful_comparisons": metrics_summary.get("comparisons", {}).get("successful", 0),
                "avg_execution_time": metrics_summary.get("comparisons", {}).get("avg_execution_time", 0),
                "total_differences": metrics_summary.get("comparisons", {}).get("total_differences", 0)
            }

        except Exception as e:
            logger.error(f"Failed to collect metrics: {e}")
            return {}

    def record_comparison_start(self, comparison_id: str, source_table: str, target_table: str):
        """记录比对开始"""
        comparison_data = {
            "comparison_id": comparison_id,
            "start_time": datetime.utcnow(),
            "status": "running",
            "source_table": source_table,
            "target_table": target_table
        }

        self.performance_monitor.record_comparison(comparison_data)

    def record_comparison_end(self, comparison_id: str, status: str,
                            execution_time: float, rows_compared: int = 0,
                            differences_found: int = 0, error_message: str = None):
        """记录比对结束"""
        comparison_data = {
            "comparison_id": comparison_id,
            "end_time": datetime.utcnow(),
            "status": status,
            "execution_time": execution_time,
            "rows_compared": rows_compared,
            "differences_found": differences_found,
            "error_message": error_message
        }

        self.performance_monitor.record_comparison(comparison_data)

    def record_metric(self, metric_type: str, metric_name: str, value: float, metadata: Optional[Dict] = None):
        """记录自定义指标"""
        self.performance_monitor.record_metric(metric_type, metric_name, value, metadata)
