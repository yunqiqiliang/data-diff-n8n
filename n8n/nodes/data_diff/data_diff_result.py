"""
Data-Diff 结果分析节点
用于分析和可视化数据比对结果
"""

from typing import Dict, Any, List, Optional
import json
import logging
from datetime import datetime
import csv
import io

from n8n_sdk import Node, NodePropertyTypes, INodeExecutionData


class DataDiffResult(Node):
    """
    Data-Diff 结果分析节点
    分析比对结果并生成报告
    """

    display_name = "Data-Diff Result"
    description = "Analyze and visualize data comparison results"
    group = "transform"
    version = 1

    # 节点输入输出配置
    inputs = ["main"]
    outputs = ["main", "report", "alerts"]

    # 节点属性配置
    properties = [
        {
            "displayName": "Analysis Type",
            "name": "analysis_type",
            "type": NodePropertyTypes.OPTIONS,
            "default": "full_analysis",
            "options": [
                {
                    "name": "Full Analysis",
                    "value": "full_analysis",
                    "description": "Complete result analysis with all metrics"
                },
                {
                    "name": "Summary Only",
                    "value": "summary_only",
                    "description": "Basic summary statistics"
                },
                {
                    "name": "Differences Only",
                    "value": "differences_only",
                    "description": "Focus on differences and anomalies"
                },
                {
                    "name": "Quality Assessment",
                    "value": "quality_assessment",
                    "description": "Data quality scoring and recommendations"
                }
            ],
            "description": "Type of analysis to perform"
        },
        {
            "displayName": "Report Template",
            "name": "report_template",
            "type": NodePropertyTypes.OPTIONS,
            "default": "standard",
            "options": [
                {"name": "Standard Report", "value": "standard"},
                {"name": "Executive Summary", "value": "executive"},
                {"name": "Technical Detail", "value": "technical"},
                {"name": "Data Quality Report", "value": "quality"},
                {"name": "Migration Validation", "value": "migration"}
            ],
            "description": "Report template to use"
        },
        {
            "displayName": "Alert Thresholds",
            "name": "alert_thresholds",
            "type": NodePropertyTypes.JSON,
            "default": "{\n  \"max_difference_rate\": 0.05,\n  \"min_match_rate\": 0.95,\n  \"max_missing_rows\": 100\n}",
            "description": "Thresholds for generating alerts"
        },
        {
            "displayName": "Include Visualizations",
            "name": "include_visualizations",
            "type": NodePropertyTypes.BOOLEAN,
            "default": True,
            "description": "Include charts and visualizations in report"
        },
        {
            "displayName": "Export Format",
            "name": "export_format",
            "type": NodePropertyTypes.OPTIONS,
            "default": "json",
            "options": [
                {"name": "JSON", "value": "json"},
                {"name": "HTML", "value": "html"},
                {"name": "PDF", "value": "pdf"},
                {"name": "Excel", "value": "excel"},
                {"name": "CSV", "value": "csv"}
            ],
            "description": "Export format for the report"
        },
        {
            "displayName": "Include Recommendations",
            "name": "include_recommendations",
            "type": NodePropertyTypes.BOOLEAN,
            "default": True,
            "description": "Include improvement recommendations"
        }
    ]

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    async def execute(self, items: List[INodeExecutionData]) -> List[List[INodeExecutionData]]:
        """
        执行结果分析节点逻辑
        返回三个输出：分析结果、报告、告警
        """
        main_results = []
        report_results = []
        alert_results = []

        for item_index, item in enumerate(items):
            try:
                # 获取参数
                analysis_type = self.get_node_parameter("analysis_type", item_index)
                report_template = self.get_node_parameter("report_template", item_index)
                alert_thresholds = self.get_node_parameter("alert_thresholds", item_index)

                # 解析告警阈值
                if isinstance(alert_thresholds, str):
                    alert_thresholds = json.loads(alert_thresholds)

                # 获取比对结果数据
                comparison_result = item.json.get("comparison_result", {})

                # 执行分析
                analysis_result = self._analyze_results(comparison_result, analysis_type)

                # 生成报告
                report = self._generate_report(analysis_result, report_template, item_index)

                # 检查告警
                alerts = self._check_alerts(analysis_result, alert_thresholds)

                # 构建输出
                main_output, report_output, alert_output = self._build_outputs(
                    analysis_result, report, alerts, item_index
                )

                main_results.append(main_output)
                report_results.append(report_output)
                alert_results.append(alert_output)

            except Exception as e:
                self.logger.error(f"Error in DataDiffResult execution: {e}")
                error_output = {
                    "json": {
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                }
                main_results.append(error_output)
                report_results.append(error_output)
                alert_results.append(error_output)

        return [main_results, report_results, alert_results]

    def _analyze_results(self, comparison_result: Dict[str, Any], analysis_type: str) -> Dict[str, Any]:
        """
        分析比对结果
        """
        if not comparison_result or comparison_result.get("status") != "completed":
            return {
                "status": "error",
                "message": "Invalid or incomplete comparison result"
            }

        statistics = comparison_result.get("statistics", {})
        differences = statistics.get("differences", {})

        # 基础分析
        total_differences = differences.get("total_differences", 0)
        rows_compared = statistics.get("rows_compared", 1)
        match_rate = statistics.get("match_rate", 0)

        analysis = {
            "status": "completed",
            "analysis_type": analysis_type,
            "timestamp": datetime.now().isoformat(),
            "basic_metrics": {
                "total_differences": total_differences,
                "match_rate": match_rate,
                "match_percentage": round(match_rate * 100, 2),
                "rows_compared": rows_compared,
                "source_rows": statistics.get("total_rows_source", 0),
                "target_rows": statistics.get("total_rows_target", 0)
            }
        }

        # 根据分析类型添加详细信息
        if analysis_type in ["full_analysis", "differences_only"]:
            analysis["difference_breakdown"] = {
                "missing_in_target": differences.get("missing_in_target", 0),
                "missing_in_source": differences.get("missing_in_source", 0),
                "value_differences": differences.get("value_differences", 0),
                "missing_in_target_pct": round((differences.get("missing_in_target", 0) / rows_compared) * 100, 2),
                "missing_in_source_pct": round((differences.get("missing_in_source", 0) / rows_compared) * 100, 2),
                "value_differences_pct": round((differences.get("value_differences", 0) / rows_compared) * 100, 2)
            }

        if analysis_type in ["full_analysis", "quality_assessment"]:
            analysis["quality_assessment"] = self._assess_data_quality(statistics, differences)

        if analysis_type == "full_analysis":
            analysis["trend_analysis"] = self._analyze_trends(comparison_result)
            analysis["performance_metrics"] = {
                "execution_time_seconds": comparison_result.get("execution_time_seconds", 0),
                "throughput_rows_per_second": round(rows_compared / max(comparison_result.get("execution_time_seconds", 1), 1), 2)
            }

        return analysis

    def _assess_data_quality(self, statistics: Dict[str, Any], differences: Dict[str, Any]) -> Dict[str, Any]:
        """
        评估数据质量
        """
        rows_compared = statistics.get("rows_compared", 1)
        total_differences = differences.get("total_differences", 0)

        # 计算质量得分
        match_rate = 1 - (total_differences / rows_compared)

        if match_rate >= 0.99:
            quality_score = "Excellent"
            quality_grade = "A"
        elif match_rate >= 0.95:
            quality_score = "Good"
            quality_grade = "B"
        elif match_rate >= 0.90:
            quality_score = "Fair"
            quality_grade = "C"
        else:
            quality_score = "Poor"
            quality_grade = "D"

        return {
            "quality_score": quality_score,
            "quality_grade": quality_grade,
            "match_rate": match_rate,
            "issues": {
                "data_missing": differences.get("missing_in_target", 0) + differences.get("missing_in_source", 0),
                "data_inconsistency": differences.get("value_differences", 0),
                "completeness_score": round(min(statistics.get("total_rows_source", 0), statistics.get("total_rows_target", 0)) / max(statistics.get("total_rows_source", 1), statistics.get("total_rows_target", 1)) * 100, 2)
            }
        }

    def _analyze_trends(self, comparison_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析趋势（模拟实现）
        """
        # 这里应该实现基于历史数据的趋势分析
        # 暂时返回模拟数据
        return {
            "trend_direction": "stable",
            "quality_trend": "improving",
            "historical_comparison": "Current match rate is 2.3% better than last week"
        }

    def _generate_report(self, analysis_result: Dict[str, Any], template: str, item_index: int) -> Dict[str, Any]:
        """
        生成分析报告
        """
        export_format = self.get_node_parameter("export_format", item_index)
        include_visualizations = self.get_node_parameter("include_visualizations", item_index)
        include_recommendations = self.get_node_parameter("include_recommendations", item_index)

        report = {
            "template": template,
            "format": export_format,
            "timestamp": datetime.now().isoformat(),
            "title": self._get_report_title(template),
            "sections": []
        }

        # 根据模板生成不同的报告内容
        if template == "executive":
            report["sections"] = self._generate_executive_summary(analysis_result)
        elif template == "technical":
            report["sections"] = self._generate_technical_report(analysis_result)
        elif template == "quality":
            report["sections"] = self._generate_quality_report(analysis_result)
        elif template == "migration":
            report["sections"] = self._generate_migration_report(analysis_result)
        else:  # standard
            report["sections"] = self._generate_standard_report(analysis_result)

        # 添加可视化
        if include_visualizations:
            report["visualizations"] = self._generate_visualizations(analysis_result)

        # 添加建议
        if include_recommendations:
            report["recommendations"] = self._generate_recommendations(analysis_result)

        return report

    def _get_report_title(self, template: str) -> str:
        """
        获取报告标题
        """
        titles = {
            "executive": "Executive Summary - Data Comparison Report",
            "technical": "Technical Analysis - Data Comparison Report",
            "quality": "Data Quality Assessment Report",
            "migration": "Data Migration Validation Report",
            "standard": "Data Comparison Analysis Report"
        }
        return titles.get(template, "Data Comparison Report")

    def _generate_executive_summary(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        生成执行摘要
        """
        metrics = analysis.get("basic_metrics", {})
        quality = analysis.get("quality_assessment", {})

        return [
            {
                "title": "Key Findings",
                "content": f"Data comparison completed with {metrics.get('match_percentage', 0)}% match rate. "
                          f"Quality assessment: {quality.get('quality_score', 'Unknown')}"
            },
            {
                "title": "Impact Assessment",
                "content": f"Out of {metrics.get('rows_compared', 0)} rows compared, "
                          f"{metrics.get('total_differences', 0)} differences were identified."
            }
        ]

    def _generate_technical_report(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        生成技术报告
        """
        sections = []

        if "basic_metrics" in analysis:
            sections.append({
                "title": "Comparison Metrics",
                "content": analysis["basic_metrics"]
            })

        if "difference_breakdown" in analysis:
            sections.append({
                "title": "Difference Analysis",
                "content": analysis["difference_breakdown"]
            })

        if "performance_metrics" in analysis:
            sections.append({
                "title": "Performance Analysis",
                "content": analysis["performance_metrics"]
            })

        return sections

    def _generate_quality_report(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        生成质量报告
        """
        quality = analysis.get("quality_assessment", {})

        return [
            {
                "title": "Data Quality Score",
                "content": {
                    "score": quality.get("quality_score"),
                    "grade": quality.get("quality_grade"),
                    "match_rate": quality.get("match_rate")
                }
            },
            {
                "title": "Quality Issues",
                "content": quality.get("issues", {})
            }
        ]

    def _generate_migration_report(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        生成迁移报告
        """
        metrics = analysis.get("basic_metrics", {})

        return [
            {
                "title": "Migration Status",
                "content": f"Migration validation shows {metrics.get('match_percentage', 0)}% data integrity"
            },
            {
                "title": "Data Completeness",
                "content": {
                    "source_rows": metrics.get("source_rows", 0),
                    "target_rows": metrics.get("target_rows", 0),
                    "completeness_rate": round((metrics.get("target_rows", 0) / max(metrics.get("source_rows", 1), 1)) * 100, 2)
                }
            }
        ]

    def _generate_standard_report(self, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        生成标准报告
        """
        sections = []

        for key, value in analysis.items():
            if key not in ["status", "analysis_type", "timestamp"]:
                sections.append({
                    "title": key.replace("_", " ").title(),
                    "content": value
                })

        return sections

    def _generate_visualizations(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成可视化配置
        """
        # 这里应该生成实际的图表配置
        # 暂时返回配置信息
        return {
            "charts": [
                {
                    "type": "pie",
                    "title": "Match vs Differences",
                    "data": {
                        "matched": analysis.get("basic_metrics", {}).get("rows_compared", 0) - analysis.get("basic_metrics", {}).get("total_differences", 0),
                        "differences": analysis.get("basic_metrics", {}).get("total_differences", 0)
                    }
                },
                {
                    "type": "bar",
                    "title": "Difference Types",
                    "data": analysis.get("difference_breakdown", {})
                }
            ]
        }

    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """
        生成改进建议
        """
        recommendations = []
        quality = analysis.get("quality_assessment", {})
        differences = analysis.get("difference_breakdown", {})

        match_rate = analysis.get("basic_metrics", {}).get("match_percentage", 100)

        if match_rate < 95:
            recommendations.append("Consider implementing data validation rules to improve data quality")

        if differences and differences.get("missing_in_target", 0) > 0:
            recommendations.append("Investigate missing records in target database")

        if differences and differences.get("value_differences", 0) > 0:
            recommendations.append("Review data transformation logic for value discrepancies")

        if quality and quality.get("quality_score") == "Poor":
            recommendations.append("Implement comprehensive data cleansing process")

        if not recommendations:
            recommendations.append("Data quality is good. Continue monitoring for consistency")

        return recommendations

    def _check_alerts(self, analysis: Dict[str, Any], thresholds: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        检查告警条件
        """
        alerts = []
        metrics = analysis.get("basic_metrics", {})

        # 检查匹配率
        match_rate = metrics.get("match_rate", 1)
        min_match_rate = thresholds.get("min_match_rate", 0.95)

        if match_rate < min_match_rate:
            alerts.append({
                "type": "warning",
                "severity": "high" if match_rate < 0.90 else "medium",
                "message": f"Match rate ({match_rate:.2%}) below threshold ({min_match_rate:.2%})",
                "metric": "match_rate",
                "value": match_rate,
                "threshold": min_match_rate
            })

        # 检查差异率
        difference_rate = metrics.get("total_differences", 0) / max(metrics.get("rows_compared", 1), 1)
        max_difference_rate = thresholds.get("max_difference_rate", 0.05)

        if difference_rate > max_difference_rate:
            alerts.append({
                "type": "warning",
                "severity": "high" if difference_rate > 0.10 else "medium",
                "message": f"Difference rate ({difference_rate:.2%}) exceeds threshold ({max_difference_rate:.2%})",
                "metric": "difference_rate",
                "value": difference_rate,
                "threshold": max_difference_rate
            })

        # 检查缺失行数
        missing_rows = analysis.get("difference_breakdown", {}).get("missing_in_target", 0)
        max_missing_rows = thresholds.get("max_missing_rows", 100)

        if missing_rows > max_missing_rows:
            alerts.append({
                "type": "error",
                "severity": "high",
                "message": f"Missing rows ({missing_rows}) exceeds threshold ({max_missing_rows})",
                "metric": "missing_rows",
                "value": missing_rows,
                "threshold": max_missing_rows
            })

        return alerts

    def _build_outputs(
        self,
        analysis: Dict[str, Any],
        report: Dict[str, Any],
        alerts: List[Dict[str, Any]],
        item_index: int
    ) -> tuple:
        """
        构建三个输出的数据
        """
        timestamp = datetime.now().isoformat()

        # 主要输出：完整分析结果
        main_output = {
            "json": {
                "analysis": analysis,
                "timestamp": timestamp
            }
        }

        # 报告输出
        report_output = {
            "json": {
                "report": report,
                "timestamp": timestamp
            }
        }

        # 告警输出
        alert_output = {
            "json": {
                "alerts": alerts,
                "alert_count": len(alerts),
                "has_critical_alerts": any(alert.get("severity") == "high" for alert in alerts),
                "timestamp": timestamp
            }
        }

        return main_output, report_output, alert_output
