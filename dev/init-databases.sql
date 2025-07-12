-- 初始化开发环境数据库
-- 创建必要的数据库和用户

-- 创建 N8N 数据库
CREATE DATABASE n8n;

-- 创建 DataDiff 数据库
CREATE DATABASE datadiff;

-- 创建测试数据库
CREATE DATABASE test_source;
CREATE DATABASE test_target;

-- 创建测试表结构
\c test_source;

CREATE SCHEMA IF NOT EXISTS public;

-- 创建测试表 - 用户表
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    last_login TIMESTAMP
);

-- 创建测试表 - 订单表
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建测试表 - 产品表
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO users (username, email, full_name, last_login) VALUES
('john_doe', 'john@example.com', 'John Doe', '2024-01-15 10:30:00'),
('jane_smith', 'jane@example.com', 'Jane Smith', '2024-01-14 15:20:00'),
('bob_wilson', 'bob@example.com', 'Bob Wilson', '2024-01-13 09:45:00'),
('alice_brown', 'alice@example.com', 'Alice Brown', '2024-01-12 14:10:00'),
('charlie_davis', 'charlie@example.com', 'Charlie Davis', '2024-01-11 11:25:00');

INSERT INTO products (sku, name, description, price, category, stock_quantity) VALUES
('LAPTOP001', 'Gaming Laptop', 'High-performance gaming laptop', 1299.99, 'Electronics', 50),
('PHONE001', 'Smartphone Pro', 'Latest smartphone with advanced features', 899.99, 'Electronics', 100),
('BOOK001', 'Data Science Guide', 'Comprehensive guide to data science', 49.99, 'Books', 200),
('CHAIR001', 'Ergonomic Office Chair', 'Comfortable office chair', 299.99, 'Furniture', 25),
('MOUSE001', 'Wireless Gaming Mouse', 'High-precision gaming mouse', 79.99, 'Electronics', 75);

INSERT INTO orders (user_id, order_number, total_amount, status) VALUES
(1, 'ORD-2024-001', 1299.99, 'completed'),
(2, 'ORD-2024-002', 949.98, 'shipped'),
(3, 'ORD-2024-003', 49.99, 'pending'),
(4, 'ORD-2024-004', 379.98, 'processing'),
(5, 'ORD-2024-005', 79.99, 'completed');

-- 切换到目标数据库并创建相似结构
\c test_target;

CREATE SCHEMA IF NOT EXISTS public;

-- 创建相同的表结构（稍有差异用于测试）
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    last_login TIMESTAMP,
    -- 新增字段用于测试模式差异
    phone VARCHAR(20)
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 新增字段
    shipping_address TEXT
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    sku VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入类似但有差异的测试数据
INSERT INTO users (username, email, full_name, last_login, phone) VALUES
('john_doe', 'john@example.com', 'John Doe', '2024-01-15 10:30:00', '555-0101'),
('jane_smith', 'jane@example.com', 'Jane Smith', '2024-01-14 15:20:00', '555-0102'),
('bob_wilson', 'bob@example.com', 'Bob Wilson', '2024-01-13 09:45:00', '555-0103'),
('alice_brown', 'alice@example.com', 'Alice Brown Updated', '2024-01-12 14:10:00', '555-0104'), -- 姓名有差异
('charlie_davis', 'charlie@example.com', 'Charlie Davis', '2024-01-11 11:25:00', '555-0105'),
('new_user', 'new@example.com', 'New User', '2024-01-16 16:00:00', '555-0106'); -- 新增用户

INSERT INTO products (sku, name, description, price, category, stock_quantity) VALUES
('LAPTOP001', 'Gaming Laptop', 'High-performance gaming laptop', 1399.99, 'Electronics', 45), -- 价格和库存有差异
('PHONE001', 'Smartphone Pro', 'Latest smartphone with advanced features', 899.99, 'Electronics', 95),
('BOOK001', 'Data Science Guide', 'Comprehensive guide to data science', 49.99, 'Books', 200),
('CHAIR001', 'Ergonomic Office Chair', 'Comfortable office chair', 299.99, 'Furniture', 25),
('MOUSE001', 'Wireless Gaming Mouse', 'High-precision gaming mouse', 79.99, 'Electronics', 75),
('TABLET001', 'Professional Tablet', 'High-end tablet for professionals', 599.99, 'Electronics', 30); -- 新增产品

INSERT INTO orders (user_id, order_number, total_amount, status, shipping_address) VALUES
(1, 'ORD-2024-001', 1399.99, 'completed', '123 Main St, City, State'), -- 金额有差异
(2, 'ORD-2024-002', 949.98, 'delivered', '456 Oak Ave, City, State'), -- 状态有差异
(3, 'ORD-2024-003', 49.99, 'cancelled', '789 Pine Rd, City, State'), -- 状态有差异
(4, 'ORD-2024-004', 379.98, 'processing', '321 Elm St, City, State'),
(5, 'ORD-2024-005', 79.99, 'completed', '654 Maple Dr, City, State'),
(6, 'ORD-2024-006', 599.99, 'pending', '987 Cedar Ln, City, State'); -- 新增订单

-- 创建 DataDiff 应用相关的表
\c datadiff;

CREATE SCHEMA IF NOT EXISTS public;

-- 创建结果存储的 schema
CREATE SCHEMA IF NOT EXISTS data_diff_results;

-- 比对历史表（保留原表用于向后兼容）
CREATE TABLE comparison_history (
    id SERIAL PRIMARY KEY,
    comparison_id UUID UNIQUE NOT NULL,
    source_connection TEXT NOT NULL,
    target_connection TEXT NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    comparison_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    execution_time_seconds DECIMAL(10, 3),
    rows_compared INTEGER,
    differences_found INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 比对结果汇总表
CREATE TABLE data_diff_results.comparison_summary (
    id SERIAL PRIMARY KEY,
    comparison_id UUID UNIQUE NOT NULL,
    source_connection JSONB NOT NULL,
    target_connection JSONB NOT NULL,
    source_table TEXT NOT NULL,
    target_table TEXT NOT NULL,
    key_columns TEXT[] NOT NULL,
    algorithm VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    execution_time_seconds DECIMAL(10, 3) NOT NULL,
    rows_compared INTEGER,
    rows_matched INTEGER,
    rows_different INTEGER,
    match_rate DECIMAL(5, 2),
    sampling_config JSONB,
    column_remapping JSONB,
    where_conditions JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 差异详情表
CREATE TABLE data_diff_results.difference_details (
    id SERIAL PRIMARY KEY,
    comparison_id UUID NOT NULL,
    row_key JSONB NOT NULL,
    difference_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    column_name VARCHAR(255),
    source_value TEXT,
    target_value TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 列统计表
CREATE TABLE data_diff_results.column_statistics (
    id SERIAL PRIMARY KEY,
    comparison_id UUID NOT NULL,
    table_side VARCHAR(10) NOT NULL CHECK (table_side IN ('source', 'target')),
    column_name VARCHAR(255) NOT NULL,
    data_type VARCHAR(100),
    null_count INTEGER,
    null_rate DECIMAL(5, 2),
    total_count INTEGER,
    unique_count INTEGER,
    cardinality DECIMAL(10, 6),
    min_value TEXT,
    max_value TEXT,
    avg_value DECIMAL(20, 6),
    avg_length DECIMAL(10, 2),
    value_distribution JSONB,
    percentiles JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 时间线分析表
CREATE TABLE data_diff_results.timeline_analysis (
    id SERIAL PRIMARY KEY,
    comparison_id UUID NOT NULL,
    time_column VARCHAR(255) NOT NULL,
    period_type VARCHAR(50) NOT NULL,
    period_start TIMESTAMP NOT NULL,
    period_end TIMESTAMP NOT NULL,
    source_count INTEGER,
    target_count INTEGER,
    matched_count INTEGER,
    difference_count INTEGER,
    match_rate DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);

-- 性能指标表
CREATE TABLE data_diff_results.performance_metrics (
    id SERIAL PRIMARY KEY,
    comparison_id UUID NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(20, 6) NOT NULL,
    metric_unit VARCHAR(50),
    metric_context JSONB,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comparison_id) REFERENCES data_diff_results.comparison_summary(comparison_id) ON DELETE CASCADE
);


-- 告警规则表
CREATE TABLE alert_rules (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(100) UNIQUE NOT NULL,
    condition_expression TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    cooldown_seconds INTEGER DEFAULT 300,
    notification_channels JSONB,
    is_enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 告警历史表
CREATE TABLE alert_history (
    id SERIAL PRIMARY KEY,
    alert_id UUID UNIQUE NOT NULL,
    rule_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    triggered_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    message TEXT,
    context JSONB,
    status VARCHAR(20) DEFAULT 'active'
);

-- 系统指标表
CREATE TABLE system_metrics (
    id SERIAL PRIMARY KEY,
    metric_type VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    value DECIMAL(15, 6) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- 创建索引以提高查询性能
CREATE INDEX idx_comparison_history_start_time ON comparison_history(start_time);
CREATE INDEX idx_comparison_history_status ON comparison_history(status);
CREATE INDEX idx_alert_history_triggered_at ON alert_history(triggered_at);
CREATE INDEX idx_system_metrics_timestamp ON system_metrics(timestamp);
CREATE INDEX idx_system_metrics_type_name ON system_metrics(metric_type, metric_name);

-- 为 data_diff_results schema 创建索引
CREATE INDEX idx_summary_comparison_id ON data_diff_results.comparison_summary(comparison_id);
CREATE INDEX idx_summary_start_time ON data_diff_results.comparison_summary(start_time);
CREATE INDEX idx_summary_tables ON data_diff_results.comparison_summary(source_table, target_table);
CREATE INDEX idx_details_comparison_id ON data_diff_results.difference_details(comparison_id);
CREATE INDEX idx_details_type_severity ON data_diff_results.difference_details(difference_type, severity);
CREATE INDEX idx_statistics_comparison_id ON data_diff_results.column_statistics(comparison_id);
CREATE INDEX idx_statistics_column ON data_diff_results.column_statistics(comparison_id, table_side, column_name);
CREATE INDEX idx_timeline_comparison_id ON data_diff_results.timeline_analysis(comparison_id);
CREATE INDEX idx_timeline_period ON data_diff_results.timeline_analysis(comparison_id, time_column, period_start);
CREATE INDEX idx_metrics_comparison_id ON data_diff_results.performance_metrics(comparison_id);
CREATE INDEX idx_metrics_name ON data_diff_results.performance_metrics(comparison_id, metric_name);

-- 创建视图以便于查询
CREATE VIEW data_diff_results.recent_comparisons AS
SELECT 
    cs.comparison_id,
    cs.source_table,
    cs.target_table,
    cs.algorithm,
    cs.start_time,
    cs.execution_time_seconds,
    cs.rows_compared,
    cs.match_rate,
    cs.rows_different,
    CASE 
        WHEN cs.match_rate >= 99.5 THEN 'Excellent'
        WHEN cs.match_rate >= 95.0 THEN 'Good'
        WHEN cs.match_rate >= 90.0 THEN 'Fair'
        ELSE 'Poor'
    END as match_quality,
    cs.created_at
FROM data_diff_results.comparison_summary cs
ORDER BY cs.start_time DESC
LIMIT 100;

-- 插入一些示例数据
INSERT INTO alert_rules (rule_name, condition_expression, severity, notification_channels) VALUES
('high_diff_count', 'differences_found > 1000', 'medium', '["email", "slack"]'::jsonb),
('comparison_failure', 'status = ''failed''', 'high', '["email", "slack", "webhook"]'::jsonb),
('long_execution', 'execution_time_seconds > 3600', 'low', '["email"]'::jsonb);

-- 授予权限
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA data_diff_results TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_diff_results TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_diff_results TO postgres;
