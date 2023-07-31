-- Создание таблицы brand
CREATE TABLE IF NOT EXISTS damaged_data.brand (
brand_id VARCHAR(255),
brand VARCHAR(255)
);

-- Создание таблицы category
CREATE TABLE IF NOT EXISTS damaged_data.category (
category_id VARCHAR(255),
category_name VARCHAR(255)
);

-- Создание таблицы product
CREATE TABLE IF NOT EXISTS damaged_data.product (
product_id VARCHAR(255),
name_short VARCHAR(255),
category_id VARCHAR(255),
pricing_line_id VARCHAR(255),
brand_id VARCHAR(255)
);

-- Создание таблицы store
CREATE TABLE IF NOT EXISTS damaged_data.store (
pos VARCHAR(255),
pos_name VARCHAR(255)
);

-- Создание таблицы transaction
CREATE TABLE IF NOT EXISTS damaged_data.transaction (
transaction_id VARCHAR(255),
product_id VARCHAR(255),
recorded_on VARCHAR(255),
quantity VARCHAR(255),
price VARCHAR(255),
price_full VARCHAR(255),
order_type_id VARCHAR(255),
pos VARCHAR(255)
);

-- Создание таблицы stock
CREATE TABLE IF NOT EXISTS damaged_data.stock (
available_on VARCHAR(255),
product_id VARCHAR(255),
pos VARCHAR(255),
available_quantity VARCHAR(255),
cost_per_item VARCHAR(255)
);