CREATE TABLE datamart.sales AS
SELECT
    t.recorded_on AS date,
    str.pos_name AS pos_name,
    c.category_name AS category_name,
    p.name_short AS name_short,
    b.brand AS brand,
    t.quantity AS sum_quantity,
    t.price AS sum_price,
    t.price_full AS sum_price_full,
    stk.available_quantity AS actual_quantity
FROM dds.transaction t
LEFT JOIN dds.product p ON p.product_id = t.product_id 
LEFT JOIN dds.category c ON c.category_id = p.category_id 
LEFT JOIN dds.brand b ON b.brand_id = p.brand_id
JOIN dds.store str ON str.pos = t.pos
JOIN dds.stock stk ON 
    stk.available_on = t.recorded_on::DATE AND
    stk.product_id = t.product_id AND
    stk.pos = t.pos
-- FROM
--     dds.product
-- LEFT JOIN dds.brand ON dds.brand.brand_id = product.brand_id
-- LEFT JOIN dds.category ON dds.category.category_id = dds.product.category_id
-- LEFT JOIN dds.transaction ON dds.transaction.product_id = dds.product.product_id
-- JOIN dds.store ON dds.transaction.pos = dds.store.pos
-- LEFT JOIN dds.stock ON dds.stock.product_id = dds.product.product_id AND dds.stock.pos = dds.store.pos AND dds.stock.available_on::DATE = dds.transaction.recorded_on::DATE


GROUP BY
    str.pos_name,
    c.category_name,
    p.name_short,
    b.brand,
    t.quantity,
    t.price,
    t.price_full,
    t.recorded_on,
    stk.available_quantity;
-- CREATE TABLE datamart.sales AS
-- SELECT
-- 	dds.transaction.recorded_on AS date,
--     dds.store.pos_name AS pos_name,
--     dds.category.category_name AS category_name,
--     dds.product.name_short AS name_short,
--     dds.brand.brand AS brand,
--     transaction.quantity AS sum_quantity,
--     transaction.price AS sum_price,
--     transaction.price_full AS sum_price_full,
--     stock.available_quantity AS actual_quantity
-- FROM dds.transaction t
-- LEFT JOIN dds.product p ON p.product_id = t.product_id 
-- LEFT JOIN dds.category c ON c.category_id = p.category_id 
-- LEFT JOIN dds.brand b ON b.brand_id = p.brand_id
-- JOIN dds.stores str ON str.pos = t.pos
-- JOIN dds.stock stk ON 
--     stk.available_on = t.recorded_on::DATE AND
--     stk.product_id = t.product_id AND
--     stk.pos = t.pos