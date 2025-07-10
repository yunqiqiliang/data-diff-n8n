"""
监控和指标收集系统
提供性能监控、健康检查、指标收集等功能
"""

import time
import psutil
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque
import asyncio
from dataclasses import dataclass, asdict
import threading


@dataclass
class PerformanceMetric:
    """性能指标数据类"""
    timestamp: datetime
    metric_name: str
    value: float
    unit: str
    tags: Dict[str, str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        return result


@dataclass
class ComparisonMetrics:
    """比对任务指标"""
    task_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None
    rows_processed: int = 0
    rows_per_second: float = 0.0
    memory_peak: float = 0.0
    cpu_usage: float = 0.0
    success: bool = True
    error_message: Optional[str] = None


class MetricsCollector:
    """指标收集器"""

    def __init__(self, max_metrics: int = 10000):
        self.max_metrics = max_metrics
        self.metrics: deque = deque(maxlen=max_metrics)
        self.comparison_metrics: Dict[str, ComparisonMetrics] = {}
        self.system_metrics: deque = deque(maxlen=1000)
        self.error_counts: defaultdict = defaultdict(int)

        # 启动系统监控线程
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._system_monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

        logging.info("MetricsCollector initialized")

    def record_metric(
        self,
        name: str,
        value: float,
        unit: str = "",
        tags: Dict[str, str] = None
    ):
        """记录指标"""
        metric = PerformanceMetric(
            timestamp=datetime.utcnow(),
            metric_name=name,
            value=value,
            unit=unit,
            tags=tags or {}
        )

        self.metrics.append(metric)

    def start_comparison_monitoring(self, task_id: str) -> ComparisonMetrics:
        """开始监控比对任务"""
        metrics = ComparisonMetrics(
            task_id=task_id,
            start_time=datetime.utcnow()
        )

        self.comparison_metrics[task_id] = metrics

        logging.info(f"Started monitoring comparison task: {task_id}")
        return metrics

    def end_comparison_monitoring(
        self,
        task_id: str,
        rows_processed: int = 0,
        success: bool = True,
        error_message: Optional[str] = None
    ):
        """结束比对任务监控"""
        if task_id not in self.comparison_metrics:
            logging.warning(f"No monitoring data found for task: {task_id}")
            return

        metrics = self.comparison_metrics[task_id]
        metrics.end_time = datetime.utcnow()
        metrics.duration = (metrics.end_time - metrics.start_time).total_seconds()
        metrics.rows_processed = rows_processed
        metrics.success = success
        metrics.error_message = error_message

        if metrics.duration > 0:
            metrics.rows_per_second = rows_processed / metrics.duration

        # 记录相关指标
        self.record_metric("comparison_duration", metrics.duration, "seconds")
        self.record_metric("comparison_rows_processed", rows_processed, "rows")
        self.record_metric("comparison_rows_per_second", metrics.rows_per_second, "rows/sec")

        if not success:
            self.error_counts[error_message or "unknown_error"] += 1

        logging.info(f"Ended monitoring comparison task: {task_id}, "
                    f"Duration: {metrics.duration:.2f}s, "
                    f"Rows/sec: {metrics.rows_per_second:.2f}")

    def get_system_metrics(self) -> Dict[str, Any]:
        """获取系统指标"""
        try:
            # CPU 使用率
            cpu_percent = psutil.cpu_percent(interval=0.1)

            # 内存使用
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_used = memory.used / (1024 * 1024 * 1024)  # GB

            # 磁盘使用
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            disk_free = disk.free / (1024 * 1024 * 1024)  # GB

            # 网络统计
            network = psutil.net_io_counters()

            metrics = {
                "timestamp": datetime.utcnow().isoformat(),
                "cpu": {
                    "percent": cpu_percent,
                    "count": psutil.cpu_count()
                },
                "memory": {
                    "percent": memory_percent,
                    "used_gb": round(memory_used, 2),
                    "total_gb": round(memory.total / (1024 * 1024 * 1024), 2)
                },
                "disk": {
                    "percent": disk_percent,
                    "free_gb": round(disk_free, 2),
                    "total_gb": round(disk.total / (1024 * 1024 * 1024), 2)
                },
                "network": {
                    "bytes_sent": network.bytes_sent,
                    "bytes_recv": network.bytes_recv,
                    "packets_sent": network.packets_sent,
                    "packets_recv": network.packets_recv
                }
            }

            return metrics

        except Exception as e:
            logging.error(f"Failed to collect system metrics: {e}")
            return {}

    def get_comparison_summary(self, hours: int = 24) -> Dict[str, Any]:
        """获取比对任务汇总"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        recent_comparisons = [
            metrics for metrics in self.comparison_metrics.values()
            if metrics.start_time >= cutoff_time
        ]

        if not recent_comparisons:
            return {
                "total_comparisons": 0,
                "successful_comparisons": 0,
                "failed_comparisons": 0,
                "success_rate": 0.0,
                "avg_duration": 0.0,
                "avg_rows_per_second": 0.0
            }

        successful = [m for m in recent_comparisons if m.success]
        failed = [m for m in recent_comparisons if not m.success]

        completed = [m for m in recent_comparisons if m.end_time is not None]
        avg_duration = sum(m.duration for m in completed) / len(completed) if completed else 0
        avg_rows_per_second = sum(m.rows_per_second for m in completed) / len(completed) if completed else 0

        return {
            "total_comparisons": len(recent_comparisons),
            "successful_comparisons": len(successful),
            "failed_comparisons": len(failed),
            "success_rate": len(successful) / len(recent_comparisons) * 100,
            "avg_duration": round(avg_duration, 2),
            "avg_rows_per_second": round(avg_rows_per_second, 2),
            "time_range": f"Last {hours} hours"
        }

    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误汇总"""
        total_errors = sum(self.error_counts.values())

        error_breakdown = dict(self.error_counts)

        return {
            "total_errors": total_errors,
            "error_breakdown": error_breakdown,
            "most_common_error": max(error_breakdown.items(), key=lambda x: x[1])[0] if error_breakdown else None
        }

    def get_metrics_summary(self, hours: int = 1) -> Dict[str, Any]:
        """获取指标汇总"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        recent_metrics = [
            metric for metric in self.metrics
            if metric.timestamp >= cutoff_time
        ]

        # 按指标名称分组
        metrics_by_name = defaultdict(list)
        for metric in recent_metrics:
            metrics_by_name[metric.metric_name].append(metric.value)

        summary = {}
        for name, values in metrics_by_name.items():
            if values:
                summary[name] = {
                    "count": len(values),
                    "avg": round(sum(values) / len(values), 2),
                    "min": round(min(values), 2),
                    "max": round(max(values), 2),
                    "last": round(values[-1], 2)
                }

        return summary

    def _system_monitor_loop(self):
        """系统监控循环"""
        while self.monitoring_active:
            try:
                metrics = self.get_system_metrics()
                if metrics:
                    self.system_metrics.append(metrics)

                time.sleep(30)  # 每30秒收集一次系统指标

            except Exception as e:
                logging.error(f"System monitoring error: {e}")
                time.sleep(60)  # 出错时等待更长时间

    def stop_monitoring(self):
        """停止监控"""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        logging.info("MetricsCollector stopped")


class HealthChecker:
    """健康检查器"""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.health_checks = []

    def add_health_check(self, name: str, check_func, critical: bool = False):
        """添加健康检查"""
        self.health_checks.append({
            "name": name,
            "check_func": check_func,
            "critical": critical
        })

    async def run_health_checks(self) -> Dict[str, Any]:
        """运行所有健康检查"""
        results = {}
        overall_healthy = True
        critical_failures = []

        for check in self.health_checks:
            try:
                start_time = time.time()
                result = await check["check_func"]()
                duration = time.time() - start_time

                is_healthy = result.get("healthy", True)

                results[check["name"]] = {
                    "healthy": is_healthy,
                    "duration": round(duration, 3),
                    "details": result.get("details", {}),
                    "critical": check["critical"]
                }

                if not is_healthy:
                    overall_healthy = False
                    if check["critical"]:
                        critical_failures.append(check["name"])

                # 记录健康检查指标
                self.metrics_collector.record_metric(
                    f"health_check_{check['name']}_duration",
                    duration,
                    "seconds"
                )

            except Exception as e:
                overall_healthy = False
                results[check["name"]] = {
                    "healthy": False,
                    "error": str(e),
                    "critical": check["critical"]
                }

                if check["critical"]:
                    critical_failures.append(check["name"])

                logging.error(f"Health check {check['name']} failed: {e}")

        return {
            "overall_healthy": overall_healthy,
            "critical_failures": critical_failures,
            "checks": results,
            "timestamp": datetime.utcnow().isoformat()
        }


class AlertManager:
    """告警管理器"""

    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.alert_rules = []
        self.active_alerts = {}

    def add_alert_rule(
        self,
        name: str,
        condition_func,
        severity: str = "warning",
        cooldown_minutes: int = 60
    ):
        """添加告警规则"""
        self.alert_rules.append({
            "name": name,
            "condition_func": condition_func,
            "severity": severity,
            "cooldown_minutes": cooldown_minutes
        })

    async def check_alerts(self):
        """检查告警条件"""
        current_time = datetime.utcnow()

        for rule in self.alert_rules:
            try:
                # 检查冷却期
                if rule["name"] in self.active_alerts:
                    last_alert = self.active_alerts[rule["name"]]["timestamp"]
                    cooldown_end = last_alert + timedelta(minutes=rule["cooldown_minutes"])

                    if current_time < cooldown_end:
                        continue  # 仍在冷却期

                # 检查告警条件
                should_alert = await rule["condition_func"]()

                if should_alert:
                    alert_data = {
                        "name": rule["name"],
                        "severity": rule["severity"],
                        "timestamp": current_time,
                        "details": should_alert if isinstance(should_alert, dict) else {}
                    }

                    self.active_alerts[rule["name"]] = alert_data

                    # 发送告警
                    await self._send_alert(alert_data)

                    logging.warning(f"Alert triggered: {rule['name']} - {rule['severity']}")

            except Exception as e:
                logging.error(f"Error checking alert rule {rule['name']}: {e}")

    async def _send_alert(self, alert_data: Dict[str, Any]):
        """发送告警（可以扩展支持多种通知方式）"""
        # 这里可以实现邮件、Slack、钉钉等通知
        logging.critical(f"ALERT: {alert_data}")

    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """获取活跃告警"""
        return list(self.active_alerts.values())


# 全局监控实例
metrics_collector = MetricsCollector()
health_checker = HealthChecker(metrics_collector)
alert_manager = AlertManager(metrics_collector)


# 预定义健康检查
async def database_health_check():
    """数据库健康检查"""
    # 这里应该检查各个数据库连接
    return {"healthy": True, "details": {"connections": "ok"}}


async def memory_health_check():
    """内存健康检查"""
    memory = psutil.virtual_memory()
    healthy = memory.percent < 90

    return {
        "healthy": healthy,
        "details": {
            "memory_percent": memory.percent,
            "threshold": 90
        }
    }


async def disk_health_check():
    """磁盘健康检查"""
    disk = psutil.disk_usage('/')
    healthy = disk.percent < 85

    return {
        "healthy": healthy,
        "details": {
            "disk_percent": disk.percent,
            "threshold": 85
        }
    }


# 注册健康检查
health_checker.add_health_check("database", database_health_check, critical=True)
health_checker.add_health_check("memory", memory_health_check, critical=False)
health_checker.add_health_check("disk", disk_health_check, critical=False)


# 预定义告警规则
async def high_memory_alert():
    """高内存使用告警"""
    memory = psutil.virtual_memory()
    if memory.percent > 85:
        return {
            "message": f"Memory usage is {memory.percent}%",
            "threshold": 85
        }
    return False


async def high_error_rate_alert():
    """高错误率告警"""
    comparison_summary = metrics_collector.get_comparison_summary(hours=1)

    if (comparison_summary["total_comparisons"] > 10 and
        comparison_summary["success_rate"] < 90):
        return {
            "message": f"Success rate is {comparison_summary['success_rate']}%",
            "threshold": 90
        }
    return False


# 注册告警规则
alert_manager.add_alert_rule("high_memory", high_memory_alert, "critical", 30)
alert_manager.add_alert_rule("high_error_rate", high_error_rate_alert, "warning", 60)
