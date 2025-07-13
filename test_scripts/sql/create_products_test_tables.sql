-- 创建测试表
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    price DECIMAL(10,2),
    quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products_copy (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    price DECIMAL(10,2),
    quantity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入测试数据
INSERT INTO products (product_name, price, quantity, created_at) VALUES
('Product A', 10.99, 100, '2024-01-01 10:00:00'),
('Product B', 20.50, 50, '2024-01-02 11:00:00'),
('Product C', 15.75, 75, '2024-01-03 12:00:00'),
('Product D', 30.00, 30, '2024-01-04 13:00:00'),
('Product E', 25.25, 60, '2024-01-05 14:00:00'),
('Product F', 40.00, 20, '2024-01-06 15:00:00');

-- 复制数据到 products_copy
INSERT INTO products_copy SELECT * FROM products;

-- 修改一条记录以创建差异
UPDATE products_copy SET price = 12.99 WHERE product_id = 1;

-- 添加一些 NULL 值用于测试列统计
UPDATE products SET quantity = NULL WHERE product_id = 3;
UPDATE products_copy SET quantity = NULL WHERE product_id = 4;