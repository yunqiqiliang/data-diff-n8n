"""
结果处理器
负责处理和转换比对结果
"""

import logging
import json
import csv
import io
from typing import Dict, Any, List, Optional
from datetime import datetime
import base64


class ResultProcessor:
    """
    结果处理器
    处理各种格式的比对结果输出
    """

    def __init__(self, config_manager=None):
        self.logger = logging.getLogger(__name__)
        self.config_manager = config_manager

    async def process_comparison_result(
        self,
        raw_result: Dict[str, Any],
        output_format: str = "json",
        include_details: bool = True
    ) -> Dict[str, Any]:
        """
        处理比对结果

        Args:
            raw_result: 原始比对结果
            output_format: 输出格式
            include_details: 是否包含详细信息

        Returns:
            处理后的结果
        """
        try:
            # 标准化结果结构
            standardized_result = self._standardize_result(raw_result)

            # 根据格式处理
            if output_format == "json":
                processed_result = self._process_json_result(standardized_result, include_details)
            elif output_format == "csv":
                processed_result = self._process_csv_result(standardized_result)
            elif output_format == "html":
                processed_result = self._process_html_result(standardized_result)
            elif output_format == "excel":
                processed_result = self._process_excel_result(standardized_result)
            else:
                processed_result = standardized_result

            self.logger.info(f"Processed result in {output_format} format")
            return processed_result

        except Exception as e:
            self.logger.error(f"Failed to process result: {e}")
            return {
                "error": f"Failed to process result: {str(e)}",
                "raw_result": raw_result
            }

    def _standardize_result(self, raw_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        标准化结果结构
        """
        # 确保有必要的字段
        standardized = {
            "status": raw_result.get("status", "unknown"),
            "job_id": raw_result.get("job_id", ""),
            "timestamp": raw_result.get("start_time", datetime.now().isoformat()),
            "execution_time_seconds": raw_result.get("execution_time_seconds", 0),
            "statistics": raw_result.get("statistics", {}),
            "sample_differences": raw_result.get("sample_differences", []),
            "summary": raw_result.get("summary", {}),
            "config": raw_result.get("config", {})
        }

        # 标准化统计信息
        stats = standardized["statistics"]
        if stats:
            standardized["statistics"] = {
                "source_table": stats.get("source_table", ""),
                "target_table": stats.get("target_table", ""),
                "total_rows_source": stats.get("total_rows_source", 0),
                "total_rows_target": stats.get("total_rows_target", 0),
                "rows_compared": stats.get("rows_compared", 0),
                "differences": stats.get("differences", {}),
                "match_rate": stats.get("match_rate", 0.0)
            }
        
        # 添加采样信息（如果有）
        if raw_result.get("config", {}).get("_sampling_applied"):
            config = raw_result.get("config", {})
            standardized["sampling_info"] = {
                "enabled": True,
                "sample_size": config.get("_actual_sample_size", 0),
                "confidence_level": config.get("_sampling_config", {}).get("confidence_level", 0.95),
                "margin_of_error": config.get("_sampling_config", {}).get("margin_of_error", 0.01),
                "source_total_rows": config.get("_source_count", 0),
                "target_total_rows": config.get("_target_count", 0)
            }
            
            # 如果有采样，添加推断统计
            if standardized["statistics"].get("differences"):
                sample_differences = sum(standardized["statistics"]["differences"].values())
                sample_size = standardized["sampling_info"]["sample_size"]
                population_size = max(
                    standardized["sampling_info"]["source_total_rows"],
                    standardized["sampling_info"]["target_total_rows"]
                )
                
                if sample_size > 0 and population_size > 0:
                    # 计算推断到总体的差异数
                    extrapolation_factor = population_size / sample_size
                    estimated_differences = int(sample_differences * extrapolation_factor)
                    
                    # 计算置信区间（简化版）
                    import math
                    confidence_level = standardized["sampling_info"]["confidence_level"]
                    z_score = 1.96 if confidence_level == 0.95 else (1.645 if confidence_level == 0.90 else 2.576)
                    sample_proportion = sample_differences / sample_size if sample_size > 0 else 0
                    standard_error = math.sqrt((sample_proportion * (1 - sample_proportion)) / sample_size) if sample_size > 1 else 0
                    margin = z_score * standard_error
                    
                    lower_bound = max(0, int((sample_proportion - margin) * population_size))
                    upper_bound = min(population_size, int((sample_proportion + margin) * population_size))
                    
                    standardized["sampling_info"]["extrapolation"] = {
                        "estimated_total_differences": estimated_differences,
                        "confidence_interval": [lower_bound, upper_bound],
                        "extrapolation_factor": round(extrapolation_factor, 2)
                    }

        return standardized

    def _process_json_result(self, result: Dict[str, Any], include_details: bool) -> Dict[str, Any]:
        """
        处理 JSON 格式结果
        """
        json_result = result.copy()

        if not include_details:
            # 移除详细信息，只保留摘要
            json_result.pop("sample_differences", None)
            json_result.pop("config", None)

        # 添加格式化的摘要
        json_result["formatted_summary"] = self._format_summary(result)

        return json_result

    def _process_csv_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理 CSV 格式结果
        """
        # 生成差异数据的 CSV
        differences = result.get("sample_differences", [])

        if not differences:
            return {
                "csv_data": "No differences found",
                "filename": f"comparison_result_{result.get('job_id', 'unknown')}.csv"
            }

        # 创建 CSV 内容
        output = io.StringIO()

        # 写入表头
        fieldnames = ["type", "key", "source_values", "target_values", "differing_columns"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        # 写入差异数据
        for diff in differences:
            row = {
                "type": diff.get("type", ""),
                "key": json.dumps(diff.get("key", {})),
                "source_values": json.dumps(diff.get("source_row", {})) if diff.get("source_row") else "",
                "target_values": json.dumps(diff.get("target_row", {})) if diff.get("target_row") else "",
                "differing_columns": ",".join(diff.get("differing_columns", []))
            }
            writer.writerow(row)

        csv_content = output.getvalue()
        output.close()

        return {
            "csv_data": csv_content,
            "filename": f"comparison_result_{result.get('job_id', 'unknown')}.csv",
            "row_count": len(differences)
        }

    def _process_html_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理 HTML 格式结果
        """
        html_content = self._generate_html_report(result)

        return {
            "html_content": html_content,
            "filename": f"comparison_report_{result.get('job_id', 'unknown')}.html"
        }

    def _process_excel_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理 Excel 格式结果（模拟实现）
        """
        # 这里应该使用 openpyxl 或类似库生成 Excel 文件
        # 暂时返回配置信息
        return {
            "excel_config": {
                "sheets": [
                    {"name": "Summary", "data": result.get("summary", {})},
                    {"name": "Statistics", "data": result.get("statistics", {})},
                    {"name": "Differences", "data": result.get("sample_differences", [])}
                ]
            },
            "filename": f"comparison_result_{result.get('job_id', 'unknown')}.xlsx"
        }

    def _format_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化摘要信息
        """
        stats = result.get("statistics", {})
        differences = stats.get("differences", {})

        return {
            "comparison_overview": {
                "source_table": stats.get("source_table"),
                "target_table": stats.get("target_table"),
                "execution_time": f"{result.get('execution_time_seconds', 0):.2f} seconds",
                "timestamp": result.get("timestamp")
            },
            "data_summary": {
                "source_rows": f"{stats.get('total_rows_source', 0):,}",
                "target_rows": f"{stats.get('total_rows_target', 0):,}",
                "rows_compared": f"{stats.get('rows_compared', 0):,}",
                "match_rate": f"{(stats.get('match_rate', 0) * 100):.2f}%"
            },
            "difference_summary": {
                "total_differences": differences.get("total_differences", 0),
                "missing_in_target": differences.get("missing_in_target", 0),
                "missing_in_source": differences.get("missing_in_source", 0),
                "value_differences": differences.get("value_differences", 0)
            },
            "quality_assessment": result.get("summary", {})
        }

    def _generate_html_report(self, result: Dict[str, Any]) -> str:
        """
        生成 HTML 报告
        """
        summary = self._format_summary(result)
        differences = result.get("sample_differences", [])

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Data Comparison Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; }}
                .section {{ margin: 20px 0; }}
                .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
                .stat-box {{ background-color: #fff; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }}
                .stat-value {{ font-size: 24px; font-weight: bold; color: #2c3e50; }}
                .stat-label {{ color: #666; font-size: 14px; }}
                .differences-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
                .differences-table th, .differences-table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .differences-table th {{ background-color: #f4f4f4; }}
                .good {{ color: #27ae60; }}
                .warning {{ color: #f39c12; }}
                .error {{ color: #e74c3c; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Data Comparison Report</h1>
                <p><strong>Job ID:</strong> {result.get('job_id', 'N/A')}</p>
                <p><strong>Generated:</strong> {result.get('timestamp', 'N/A')}</p>
                <p><strong>Execution Time:</strong> {result.get('execution_time_seconds', 0):.2f} seconds</p>
            </div>

            <div class="section">
                <h2>Comparison Overview</h2>
                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="stat-value">{summary['data_summary']['source_rows']}</div>
                        <div class="stat-label">Source Rows</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-value">{summary['data_summary']['target_rows']}</div>
                        <div class="stat-label">Target Rows</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-value">{summary['data_summary']['match_rate']}</div>
                        <div class="stat-label">Match Rate</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-value">{summary['difference_summary']['total_differences']}</div>
                        <div class="stat-label">Total Differences</div>
                    </div>
                </div>
            </div>

            <div class="section">
                <h2>Difference Breakdown</h2>
                <ul>
                    <li>Missing in Target: {summary['difference_summary']['missing_in_target']}</li>
                    <li>Missing in Source: {summary['difference_summary']['missing_in_source']}</li>
                    <li>Value Differences: {summary['difference_summary']['value_differences']}</li>
                </ul>
            </div>
        """

        # 添加差异详情表格
        if differences:
            html += """
            <div class="section">
                <h2>Sample Differences</h2>
                <table class="differences-table">
                    <tr>
                        <th>Type</th>
                        <th>Key</th>
                        <th>Source Value</th>
                        <th>Target Value</th>
                        <th>Differing Columns</th>
                    </tr>
            """

            for diff in differences[:50]:  # 限制显示前50个差异
                source_val = json.dumps(diff.get("source_row", {})) if diff.get("source_row") else "N/A"
                target_val = json.dumps(diff.get("target_row", {})) if diff.get("target_row") else "N/A"

                html += f"""
                    <tr>
                        <td>{diff.get('type', '')}</td>
                        <td>{json.dumps(diff.get('key', {}))}</td>
                        <td>{source_val[:100]}{'...' if len(source_val) > 100 else ''}</td>
                        <td>{target_val[:100]}{'...' if len(target_val) > 100 else ''}</td>
                        <td>{', '.join(diff.get('differing_columns', []))}</td>
                    </tr>
                """

            html += "</table></div>"

        html += """
        </body>
        </html>
        """

        return html

    def generate_executive_summary(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        生成执行摘要
        """
        stats = result.get("statistics", {})
        match_rate = stats.get("match_rate", 0) * 100
        total_differences = stats.get("differences", {}).get("total_differences", 0)

        # 确定数据质量等级
        if match_rate >= 99:
            quality_level = "Excellent"
            quality_color = "green"
        elif match_rate >= 95:
            quality_level = "Good"
            quality_color = "blue"
        elif match_rate >= 90:
            quality_level = "Fair"
            quality_color = "orange"
        else:
            quality_level = "Poor"
            quality_color = "red"

        return {
            "executive_summary": {
                "overall_assessment": quality_level,
                "match_percentage": f"{match_rate:.2f}%",
                "total_issues": total_differences,
                "recommendation": self._get_recommendation(match_rate, total_differences),
                "quality_color": quality_color
            },
            "key_metrics": {
                "data_integrity": f"{match_rate:.1f}%",
                "completeness": f"{min(stats.get('total_rows_source', 0), stats.get('total_rows_target', 0)) / max(stats.get('total_rows_source', 1), stats.get('total_rows_target', 1)) * 100:.1f}%",
                "processing_time": f"{result.get('execution_time_seconds', 0):.1f}s"
            },
            "action_items": self._generate_action_items(result)
        }

    def _get_recommendation(self, match_rate: float, total_differences: int) -> str:
        """
        获取建议
        """
        if match_rate >= 99:
            return "Data quality is excellent. Continue monitoring."
        elif match_rate >= 95:
            return "Data quality is good. Minor issues detected that should be investigated."
        elif match_rate >= 90:
            return "Data quality needs improvement. Review data pipeline and validation rules."
        else:
            return "Data quality is poor. Immediate action required to fix data inconsistencies."

    def _generate_action_items(self, result: Dict[str, Any]) -> List[str]:
        """
        生成行动项
        """
        action_items = []
        stats = result.get("statistics", {})
        differences = stats.get("differences", {})

        if differences.get("missing_in_target", 0) > 0:
            action_items.append(f"Investigate {differences['missing_in_target']} missing records in target")

        if differences.get("missing_in_source", 0) > 0:
            action_items.append(f"Investigate {differences['missing_in_source']} extra records in target")

        if differences.get("value_differences", 0) > 0:
            action_items.append(f"Review {differences['value_differences']} value discrepancies")

        match_rate = stats.get("match_rate", 0) * 100
        if match_rate < 95:
            action_items.append("Implement data validation rules")

        if not action_items:
            action_items.append("Continue regular monitoring")

        return action_items
