"""
工作流调度器模块

提供多种调度策略：
- 定时调度（Cron）
- 事件驱动调度
- 手动触发调度
- 依赖调度

作者: Clickzetta 数据比对团队
版本: 1.0.0
"""

from typing import Dict, List, Optional, Callable, Any, Union
from datetime import datetime, timedelta
import asyncio
import logging
from enum import Enum
from dataclasses import dataclass, field
from croniter import croniter
import uuid
import threading
import time
import json
from pathlib import Path

from .config_manager import ConfigManager
from .error_handler import ErrorHandler, DataDiffError
from ..workflows.workflow_builder import WorkflowBuilder


class ScheduleType(Enum):
    """调度类型枚举"""
    CRON = "cron"           # Cron 表达式调度
    INTERVAL = "interval"    # 间隔调度
    ONCE = "once"           # 单次执行
    EVENT = "event"         # 事件驱动
    MANUAL = "manual"       # 手动触发
    DEPENDENCY = "dependency"  # 依赖调度


class ScheduleStatus(Enum):
    """调度状态枚举"""
    PENDING = "pending"      # 等待中
    RUNNING = "running"      # 运行中
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"        # 失败
    PAUSED = "paused"        # 暂停
    CANCELLED = "cancelled"  # 已取消


@dataclass
class ScheduleConfig:
    """调度配置类"""
    schedule_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    schedule_type: ScheduleType = ScheduleType.MANUAL

    # Cron 配置
    cron_expression: Optional[str] = None
    timezone: str = "UTC"

    # 间隔配置
    interval_seconds: Optional[int] = None

    # 单次执行配置
    execute_at: Optional[datetime] = None

    # 事件配置
    event_source: Optional[str] = None
    event_conditions: Dict[str, Any] = field(default_factory=dict)

    # 依赖配置
    dependencies: List[str] = field(default_factory=list)

    # 工作流配置
    workflow_template: str = ""
    workflow_params: Dict[str, Any] = field(default_factory=dict)

    # 执行配置
    max_retries: int = 3
    retry_delay: int = 60
    timeout: int = 3600

    # 状态
    status: ScheduleStatus = ScheduleStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionRecord:
    """执行记录类"""
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    schedule_id: str = ""

    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    duration: Optional[float] = None

    status: ScheduleStatus = ScheduleStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    # 重试信息
    retry_count: int = 0
    max_retries: int = 3

    # 元数据
    metadata: Dict[str, Any] = field(default_factory=dict)


class WorkflowScheduler:
    """工作流调度器

    支持多种调度策略和执行管理功能
    """

    def __init__(self, config_manager: Optional[ConfigManager] = None):
        """初始化调度器

        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager or ConfigManager()
        self.error_handler = ErrorHandler()
        self.workflow_builder = WorkflowBuilder()

        # 调度器状态
        self._schedules: Dict[str, ScheduleConfig] = {}
        self._executions: Dict[str, ExecutionRecord] = {}
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None

        # 事件回调
        self._event_handlers: Dict[str, List[Callable]] = {}

        # 依赖管理
        self._dependency_graph: Dict[str, List[str]] = {}

        # 日志记录
        self.logger = logging.getLogger(__name__)

        # 持久化配置
        self._persistence_enabled = self.config_manager.get_config(
            "scheduler.persistence_enabled", False
        )
        self._storage_path = Path(
            self.config_manager.get_config(
                "scheduler.storage_path",
                "storage/schedules"
            )
        )

        if self._persistence_enabled:
            self._storage_path.mkdir(parents=True, exist_ok=True)
            self._load_schedules()

    async def initialize(self):
        """
        异步初始化方法（占位），兼容 FastAPI 生命周期钩子
        """
        pass

    async def shutdown(self):
        """
        异步关闭方法，兼容 FastAPI 生命周期钩子
        """
        try:
            # 停止调度器
            self.stop_scheduler()
            self.logger.info("WorkflowScheduler shutdown completed successfully")
        except Exception as e:
            self.logger.error(f"Error during WorkflowScheduler shutdown: {e}")
            raise

    def create_schedule(
        self,
        name: str,
        workflow_template: str,
        schedule_type: ScheduleType = ScheduleType.MANUAL,
        **kwargs
    ) -> str:
        """创建调度任务

        Args:
            name: 调度任务名称
            workflow_template: 工作流模板名称
            schedule_type: 调度类型
            **kwargs: 其他调度配置参数

        Returns:
            调度任务ID

        Raises:
            DataDiffError: 创建失败时抛出异常
        """
        try:
            config = ScheduleConfig(
                name=name,
                workflow_template=workflow_template,
                schedule_type=schedule_type,
                **kwargs
            )

            # 验证配置
            self._validate_schedule_config(config)

            # 计算下次执行时间
            config.next_run = self._calculate_next_run(config)

            # 存储调度配置
            self._schedules[config.schedule_id] = config

            # 构建依赖图
            if config.dependencies:
                self._update_dependency_graph(config)

            # 持久化
            if self._persistence_enabled:
                self._save_schedule(config)

            self.logger.info(f"创建调度任务: {name} (ID: {config.schedule_id})")
            return config.schedule_id

        except Exception as e:
            error_msg = f"创建调度任务失败: {str(e)}"
            self.error_handler.handle_error(
                DataDiffError(error_msg),
                {"schedule_name": name, "workflow_template": workflow_template}
            )
            raise DataDiffError(error_msg) from e

    def start_scheduler(self) -> None:
        """启动调度器"""
        if self._running:
            self.logger.warning("调度器已在运行中")
            return

        self._running = True
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True
        )
        self._scheduler_thread.start()
        self.logger.info("调度器已启动")

    def stop_scheduler(self) -> None:
        """停止调度器"""
        if not self._running:
            return

        self._running = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=10)

        self.logger.info("调度器已停止")

    def execute_schedule(
        self,
        schedule_id: str,
        force: bool = False
    ) -> str:
        """执行调度任务

        Args:
            schedule_id: 调度任务ID
            force: 是否强制执行（忽略状态检查）

        Returns:
            执行记录ID

        Raises:
            DataDiffError: 执行失败时抛出异常
        """
        if schedule_id not in self._schedules:
            raise DataDiffError(f"调度任务不存在: {schedule_id}")

        schedule = self._schedules[schedule_id]

        # 检查是否可以执行
        if not force and not self._can_execute(schedule):
            raise DataDiffError(f"调度任务当前无法执行: {schedule.name}")

        # 创建执行记录
        execution = ExecutionRecord(
            schedule_id=schedule_id,
            max_retries=schedule.max_retries
        )

        self._executions[execution.execution_id] = execution

        # 异步执行
        threading.Thread(
            target=self._execute_workflow,
            args=(schedule, execution),
            daemon=True
        ).start()

        return execution.execution_id

    def pause_schedule(self, schedule_id: str) -> None:
        """暂停调度任务"""
        if schedule_id not in self._schedules:
            raise DataDiffError(f"调度任务不存在: {schedule_id}")

        schedule = self._schedules[schedule_id]
        schedule.status = ScheduleStatus.PAUSED
        schedule.updated_at = datetime.now()

        if self._persistence_enabled:
            self._save_schedule(schedule)

        self.logger.info(f"暂停调度任务: {schedule.name}")

    def resume_schedule(self, schedule_id: str) -> None:
        """恢复调度任务"""
        if schedule_id not in self._schedules:
            raise DataDiffError(f"调度任务不存在: {schedule_id}")

        schedule = self._schedules[schedule_id]
        schedule.status = ScheduleStatus.PENDING
        schedule.updated_at = datetime.now()
        schedule.next_run = self._calculate_next_run(schedule)

        if self._persistence_enabled:
            self._save_schedule(schedule)

        self.logger.info(f"恢复调度任务: {schedule.name}")

    def delete_schedule(self, schedule_id: str) -> None:
        """删除调度任务"""
        if schedule_id not in self._schedules:
            raise DataDiffError(f"调度任务不存在: {schedule_id}")

        schedule = self._schedules[schedule_id]

        # 取消正在运行的执行
        for execution in self._executions.values():
            if (execution.schedule_id == schedule_id and
                execution.status == ScheduleStatus.RUNNING):
                execution.status = ScheduleStatus.CANCELLED

        # 删除调度配置
        del self._schedules[schedule_id]

        # 更新依赖图
        self._remove_from_dependency_graph(schedule_id)

        # 删除持久化文件
        if self._persistence_enabled:
            schedule_file = self._storage_path / f"{schedule_id}.json"
            if schedule_file.exists():
                schedule_file.unlink()

        self.logger.info(f"删除调度任务: {schedule.name}")

    def get_schedule(self, schedule_id: str) -> Optional[ScheduleConfig]:
        """获取调度配置"""
        return self._schedules.get(schedule_id)

    def list_schedules(
        self,
        status: Optional[ScheduleStatus] = None
    ) -> List[ScheduleConfig]:
        """列出调度任务

        Args:
            status: 过滤状态

        Returns:
            调度任务列表
        """
        schedules = list(self._schedules.values())

        if status:
            schedules = [s for s in schedules if s.status == status]

        return sorted(schedules, key=lambda x: x.created_at, reverse=True)

    def get_execution_history(
        self,
        schedule_id: Optional[str] = None,
        limit: int = 100
    ) -> List[ExecutionRecord]:
        """获取执行历史

        Args:
            schedule_id: 调度任务ID（可选）
            limit: 限制返回数量

        Returns:
            执行记录列表
        """
        executions = list(self._executions.values())

        if schedule_id:
            executions = [e for e in executions if e.schedule_id == schedule_id]

        executions = sorted(executions, key=lambda x: x.start_time, reverse=True)
        return executions[:limit]

    def register_event_handler(
        self,
        event_source: str,
        handler: Callable[[Dict[str, Any]], None]
    ) -> None:
        """注册事件处理器

        Args:
            event_source: 事件源名称
            handler: 事件处理函数
        """
        if event_source not in self._event_handlers:
            self._event_handlers[event_source] = []

        self._event_handlers[event_source].append(handler)
        self.logger.info(f"注册事件处理器: {event_source}")

    def trigger_event(
        self,
        event_source: str,
        event_data: Dict[str, Any]
    ) -> List[str]:
        """触发事件

        Args:
            event_source: 事件源名称
            event_data: 事件数据

        Returns:
            触发的执行ID列表
        """
        execution_ids = []

        # 查找匹配的事件调度
        for schedule in self._schedules.values():
            if (schedule.schedule_type == ScheduleType.EVENT and
                schedule.event_source == event_source and
                self._match_event_conditions(schedule, event_data)):

                try:
                    execution_id = self.execute_schedule(schedule.schedule_id)
                    execution_ids.append(execution_id)
                except Exception as e:
                    self.logger.error(f"事件触发执行失败: {str(e)}")

        # 调用事件处理器
        if event_source in self._event_handlers:
            for handler in self._event_handlers[event_source]:
                try:
                    handler(event_data)
                except Exception as e:
                    self.logger.error(f"事件处理器执行失败: {str(e)}")

        return execution_ids

    def _scheduler_loop(self) -> None:
        """调度器主循环"""
        self.logger.info("调度器主循环已启动")

        while self._running:
            try:
                current_time = datetime.now()

                # 检查需要执行的调度任务
                for schedule in self._schedules.values():
                    if self._should_execute(schedule, current_time):
                        try:
                            self.execute_schedule(schedule.schedule_id)
                        except Exception as e:
                            self.logger.error(f"调度执行失败: {str(e)}")

                # 清理过期的执行记录
                self._cleanup_executions()

                # 等待下次检查
                time.sleep(self.config_manager.get_config(
                    "scheduler.check_interval", 60
                ))

            except Exception as e:
                self.logger.error(f"调度器循环异常: {str(e)}")
                time.sleep(30)

        self.logger.info("调度器主循环已停止")

    def _validate_schedule_config(self, config: ScheduleConfig) -> None:
        """验证调度配置"""
        if not config.name:
            raise DataDiffError("调度任务名称不能为空")

        if not config.workflow_template:
            raise DataDiffError("工作流模板不能为空")

        if config.schedule_type == ScheduleType.CRON:
            if not config.cron_expression:
                raise DataDiffError("Cron 调度必须提供 cron_expression")
            if not croniter.is_valid(config.cron_expression):
                raise DataDiffError("无效的 Cron 表达式")

        elif config.schedule_type == ScheduleType.INTERVAL:
            if not config.interval_seconds or config.interval_seconds <= 0:
                raise DataDiffError("间隔调度必须提供有效的 interval_seconds")

        elif config.schedule_type == ScheduleType.ONCE:
            if not config.execute_at:
                raise DataDiffError("单次执行必须提供 execute_at 时间")
            if config.execute_at <= datetime.now():
                raise DataDiffError("执行时间必须是未来时间")

        elif config.schedule_type == ScheduleType.EVENT:
            if not config.event_source:
                raise DataDiffError("事件调度必须提供 event_source")

    def _calculate_next_run(self, config: ScheduleConfig) -> Optional[datetime]:
        """计算下次执行时间"""
        if config.schedule_type == ScheduleType.CRON:
            cron = croniter(config.cron_expression, datetime.now())
            return cron.get_next(datetime)

        elif config.schedule_type == ScheduleType.INTERVAL:
            base_time = config.last_run or datetime.now()
            return base_time + timedelta(seconds=config.interval_seconds)

        elif config.schedule_type == ScheduleType.ONCE:
            return config.execute_at

        return None

    def _should_execute(self, schedule: ScheduleConfig, current_time: datetime) -> bool:
        """判断是否应该执行"""
        if schedule.status != ScheduleStatus.PENDING:
            return False

        if not schedule.next_run:
            return False

        if current_time < schedule.next_run:
            return False

        # 检查依赖
        if schedule.dependencies:
            return self._check_dependencies(schedule)

        return True

    def _can_execute(self, schedule: ScheduleConfig) -> bool:
        """检查是否可以执行"""
        if schedule.status in [ScheduleStatus.PAUSED, ScheduleStatus.CANCELLED]:
            return False

        # 检查依赖
        if schedule.dependencies:
            return self._check_dependencies(schedule)

        return True

    def _check_dependencies(self, schedule: ScheduleConfig) -> bool:
        """检查依赖是否满足"""
        for dep_id in schedule.dependencies:
            if dep_id not in self._schedules:
                continue

            # 检查依赖任务的最近执行状态
            dep_executions = [
                e for e in self._executions.values()
                if e.schedule_id == dep_id
            ]

            if not dep_executions:
                return False

            latest_execution = max(dep_executions, key=lambda x: x.start_time)
            if latest_execution.status != ScheduleStatus.COMPLETED:
                return False

        return True

    def _execute_workflow(
        self,
        schedule: ScheduleConfig,
        execution: ExecutionRecord
    ) -> None:
        """执行工作流"""
        execution.status = ScheduleStatus.RUNNING

        try:
            # 构建工作流
            workflow_def = self.workflow_builder.create_workflow(
                template_name=schedule.workflow_template,
                **schedule.workflow_params
            )

            # 这里应该调用实际的工作流执行引擎
            # 为了演示，我们模拟执行过程
            self.logger.info(f"开始执行工作流: {schedule.name}")

            # 模拟执行时间
            time.sleep(2)

            # 模拟执行结果
            result = {
                "workflow_id": workflow_def.get("id"),
                "status": "success",
                "message": "工作流执行成功",
                "execution_time": 2.0
            }

            # 更新执行记录
            execution.end_time = datetime.now()
            execution.duration = (execution.end_time - execution.start_time).total_seconds()
            execution.status = ScheduleStatus.COMPLETED
            execution.result = result

            # 更新调度配置
            schedule.last_run = execution.start_time
            schedule.next_run = self._calculate_next_run(schedule)
            schedule.updated_at = datetime.now()

            self.logger.info(f"工作流执行成功: {schedule.name}")

        except Exception as e:
            execution.end_time = datetime.now()
            execution.duration = (execution.end_time - execution.start_time).total_seconds()
            execution.status = ScheduleStatus.FAILED
            execution.error = str(e)

            # 处理重试
            if execution.retry_count < execution.max_retries:
                execution.retry_count += 1
                execution.status = ScheduleStatus.PENDING

                # 延迟重试
                threading.Timer(
                    schedule.retry_delay,
                    lambda: self._execute_workflow(schedule, execution)
                ).start()

                self.logger.warning(
                    f"工作流执行失败，将重试 ({execution.retry_count}/{execution.max_retries}): {schedule.name}"
                )
            else:
                self.logger.error(f"工作流执行失败: {schedule.name}, 错误: {str(e)}")

        finally:
            # 持久化
            if self._persistence_enabled:
                self._save_schedule(schedule)

    def _match_event_conditions(
        self,
        schedule: ScheduleConfig,
        event_data: Dict[str, Any]
    ) -> bool:
        """匹配事件条件"""
        if not schedule.event_conditions:
            return True

        for key, expected_value in schedule.event_conditions.items():
            if key not in event_data:
                return False

            actual_value = event_data[key]
            if isinstance(expected_value, dict):
                # 支持比较操作符
                for op, value in expected_value.items():
                    if op == "eq" and actual_value != value:
                        return False
                    elif op == "gt" and actual_value <= value:
                        return False
                    elif op == "lt" and actual_value >= value:
                        return False
                    elif op == "in" and actual_value not in value:
                        return False
            else:
                if actual_value != expected_value:
                    return False

        return True

    def _update_dependency_graph(self, schedule: ScheduleConfig) -> None:
        """更新依赖图"""
        self._dependency_graph[schedule.schedule_id] = schedule.dependencies

    def _remove_from_dependency_graph(self, schedule_id: str) -> None:
        """从依赖图中移除"""
        if schedule_id in self._dependency_graph:
            del self._dependency_graph[schedule_id]

        # 移除其他任务对此任务的依赖
        for deps in self._dependency_graph.values():
            if schedule_id in deps:
                deps.remove(schedule_id)

    def _cleanup_executions(self) -> None:
        """清理过期的执行记录"""
        retention_days = self.config_manager.get_config(
            "scheduler.execution_retention_days", 30
        )
        cutoff_time = datetime.now() - timedelta(days=retention_days)

        expired_ids = [
            execution_id for execution_id, execution in self._executions.items()
            if execution.start_time < cutoff_time
        ]

        for execution_id in expired_ids:
            del self._executions[execution_id]

    def _save_schedule(self, schedule: ScheduleConfig) -> None:
        """保存调度配置到文件"""
        schedule_file = self._storage_path / f"{schedule.schedule_id}.json"

        # 序列化配置
        data = {
            "schedule_id": schedule.schedule_id,
            "name": schedule.name,
            "description": schedule.description,
            "schedule_type": schedule.schedule_type.value,
            "cron_expression": schedule.cron_expression,
            "timezone": schedule.timezone,
            "interval_seconds": schedule.interval_seconds,
            "execute_at": schedule.execute_at.isoformat() if schedule.execute_at else None,
            "event_source": schedule.event_source,
            "event_conditions": schedule.event_conditions,
            "dependencies": schedule.dependencies,
            "workflow_template": schedule.workflow_template,
            "workflow_params": schedule.workflow_params,
            "max_retries": schedule.max_retries,
            "retry_delay": schedule.retry_delay,
            "timeout": schedule.timeout,
            "status": schedule.status.value,
            "created_at": schedule.created_at.isoformat(),
            "updated_at": schedule.updated_at.isoformat(),
            "last_run": schedule.last_run.isoformat() if schedule.last_run else None,
            "next_run": schedule.next_run.isoformat() if schedule.next_run else None,
            "metadata": schedule.metadata
        }

        with open(schedule_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _load_schedules(self) -> None:
        """从文件加载调度配置"""
        if not self._storage_path.exists():
            return

        for schedule_file in self._storage_path.glob("*.json"):
            try:
                with open(schedule_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                schedule = ScheduleConfig(
                    schedule_id=data["schedule_id"],
                    name=data["name"],
                    description=data["description"],
                    schedule_type=ScheduleType(data["schedule_type"]),
                    cron_expression=data["cron_expression"],
                    timezone=data["timezone"],
                    interval_seconds=data["interval_seconds"],
                    execute_at=datetime.fromisoformat(data["execute_at"]) if data["execute_at"] else None,
                    event_source=data["event_source"],
                    event_conditions=data["event_conditions"],
                    dependencies=data["dependencies"],
                    workflow_template=data["workflow_template"],
                    workflow_params=data["workflow_params"],
                    max_retries=data["max_retries"],
                    retry_delay=data["retry_delay"],
                    timeout=data["timeout"],
                    status=ScheduleStatus(data["status"]),
                    created_at=datetime.fromisoformat(data["created_at"]),
                    updated_at=datetime.fromisoformat(data["updated_at"]),
                    last_run=datetime.fromisoformat(data["last_run"]) if data["last_run"] else None,
                    next_run=datetime.fromisoformat(data["next_run"]) if data["next_run"] else None,
                    metadata=data["metadata"]
                )

                self._schedules[schedule.schedule_id] = schedule

                # 重建依赖图
                if schedule.dependencies:
                    self._update_dependency_graph(schedule)

            except Exception as e:
                self.logger.error(f"加载调度配置失败: {schedule_file}, 错误: {str(e)}")


# 便捷函数
def create_cron_schedule(
    name: str,
    workflow_template: str,
    cron_expression: str,
    scheduler: Optional[WorkflowScheduler] = None,
    **kwargs
) -> str:
    """创建 Cron 调度任务

    Args:
        name: 任务名称
        workflow_template: 工作流模板
        cron_expression: Cron 表达式
        scheduler: 调度器实例
        **kwargs: 其他参数

    Returns:
        调度任务ID
    """
    if scheduler is None:
        scheduler = WorkflowScheduler()

    return scheduler.create_schedule(
        name=name,
        workflow_template=workflow_template,
        schedule_type=ScheduleType.CRON,
        cron_expression=cron_expression,
        **kwargs
    )


def create_interval_schedule(
    name: str,
    workflow_template: str,
    interval_seconds: int,
    scheduler: Optional[WorkflowScheduler] = None,
    **kwargs
) -> str:
    """创建间隔调度任务

    Args:
        name: 任务名称
        workflow_template: 工作流模板
        interval_seconds: 间隔秒数
        scheduler: 调度器实例
        **kwargs: 其他参数

    Returns:
        调度任务ID
    """
    if scheduler is None:
        scheduler = WorkflowScheduler()

    return scheduler.create_schedule(
        name=name,
        workflow_template=workflow_template,
        schedule_type=ScheduleType.INTERVAL,
        interval_seconds=interval_seconds,
        **kwargs
    )


def create_event_schedule(
    name: str,
    workflow_template: str,
    event_source: str,
    event_conditions: Optional[Dict[str, Any]] = None,
    scheduler: Optional[WorkflowScheduler] = None,
    **kwargs
) -> str:
    """创建事件调度任务

    Args:
        name: 任务名称
        workflow_template: 工作流模板
        event_source: 事件源
        event_conditions: 事件条件
        scheduler: 调度器实例
        **kwargs: 其他参数

    Returns:
        调度任务ID
    """
    if scheduler is None:
        scheduler = WorkflowScheduler()

    return scheduler.create_schedule(
        name=name,
        workflow_template=workflow_template,
        schedule_type=ScheduleType.EVENT,
        event_source=event_source,
        event_conditions=event_conditions or {},
        **kwargs
    )
