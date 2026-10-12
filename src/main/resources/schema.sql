CREATE TABLE IF NOT EXISTS items (
                                     data_key VARCHAR(200) PRIMARY KEY,
    data_value TEXT NOT NULL
    );

INSERT INTO items (data_key, data_value)
VALUES
    ('merchant-101', 'Merchant One'),
    ('merchant-102', 'Merchant Two'),
    ('merchant-103', 'Merchant Three')
    ON CONFLICT (data_key) DO NOTHING;

INSERT INTO items (data_key, data_value)
SELECT
    'merchant-' || n,
    'Merchant ' || n
FROM generate_series(1, 10000) AS n
    ON CONFLICT (data_key) DO NOTHING;
