-- Create test tables for metrics verification
CREATE TABLE IF NOT EXISTS test_source (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS test_target (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO test_source (id, name, email) VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com'),
(3, 'Charlie', 'charlie@example.com'),
(4, 'David', 'david@example.com'),
(5, 'Eve', 'eve@example.com');

INSERT INTO test_target (id, name, email) VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com'),
(3, 'Charlie Updated', 'charlie@example.com'),  -- Different name
(4, 'David', 'david.new@example.com'),  -- Different email
(6, 'Frank', 'frank@example.com');  -- New record not in source