-- Создание таблицы brand
CREATE TABLE IF NOT EXISTS dds.brand (
brand_id INT PRIMARY KEY,
brand VARCHAR(255)
);

-- Создание таблицы category
CREATE TABLE IF NOT EXISTS dds.category (
category_id VARCHAR(255) PRIMARY KEY,
category_name VARCHAR(255)
);

-- Создание таблицы product
CREATE TABLE IF NOT EXISTS dds.product (
product_id INT PRIMARY KEY,
name_short VARCHAR(255),
category_id VARCHAR(255),
pricing_line_id INT,
brand_id INT,
FOREIGN KEY (category_id) REFERENCES dds.category(category_id),
FOREIGN KEY (brand_id) REFERENCES dds.brand(brand_id)
);

-- Создание таблицы store
CREATE TABLE IF NOT EXISTS dds.store (
pos VARCHAR(255) PRIMARY KEY,
pos_name VARCHAR(255)
);

-- Создание таблицы transaction
CREATE TABLE IF NOT EXISTS dds.transaction (
transaction_id VARCHAR(255) PRIMARY KEY,
product_id INT,
recorded_on TIMESTAMP,
quantity INT,
price INT,
price_full INT,
order_type_id VARCHAR(255),
pos VARCHAR(255),
FOREIGN KEY (product_id) REFERENCES dds.product(product_id),
FOREIGN KEY (pos) REFERENCES dds.store(pos)
);

-- Создание таблицы stock
CREATE TABLE IF NOT EXISTS dds.stock (
available_on TIMESTAMP,
product_id INT,
pos VARCHAR(255),
available_quantity DECIMAL,
cost_per_item DECIMAL,
PRIMARY KEY (available_on, product_id, pos),
FOREIGN KEY (product_id) REFERENCES dds.product(product_id),
FOREIGN KEY (pos) REFERENCES dds.store(pos)
);

