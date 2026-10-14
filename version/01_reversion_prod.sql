
-- REVERSIÓN DEL PROYECTO EN PRODUCCIÓN

-- NO EJECUTAR COMO PARTE DEL ETL NORMAL
-- Elimina las tablas y esquemas del proyecto.

-- 1. Eliminar capa GOLD
DROP TABLE IF EXISTS dbw_supermarket_prod.gold.productos_populares;
DROP TABLE IF EXISTS dbw_supermarket_prod.gold.departamentos;
DROP TABLE IF EXISTS dbw_supermarket_prod.gold.pedidos_por_hora;

-- 2. Eliminar capa SILVER
DROP TABLE IF EXISTS dbw_supermarket_prod.silver.compras;
DROP TABLE IF EXISTS dbw_supermarket_prod.silver.pedidos;
DROP TABLE IF EXISTS dbw_supermarket_prod.silver.productos;
DROP TABLE IF EXISTS dbw_supermarket_prod.silver.auditoria_relaciones;

-- 3. Eliminar capa BRONZE
DROP TABLE IF EXISTS dbw_supermarket_prod.bronze.aisles;
DROP TABLE IF EXISTS dbw_supermarket_prod.bronze.departments;
DROP TABLE IF EXISTS dbw_supermarket_prod.bronze.products;
DROP TABLE IF EXISTS dbw_supermarket_prod.bronze.orders;
DROP TABLE IF EXISTS dbw_supermarket_prod.bronze.order_products__prior;
DROP TABLE IF EXISTS dbw_supermarket_prod.bronze.order_products__train;

-- 4. Eliminar esquemas vacíos
DROP SCHEMA IF EXISTS dbw_supermarket_prod.gold;
DROP SCHEMA IF EXISTS dbw_supermarket_prod.silver;
DROP SCHEMA IF EXISTS dbw_supermarket_prod.bronze;
