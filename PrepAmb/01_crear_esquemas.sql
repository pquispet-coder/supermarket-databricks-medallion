-- PREPARACIÓN DEL AMBIENTE PROD
-- Catálogo existente: dbw_supermarket_prod

CREATE SCHEMA IF NOT EXISTS
    dbw_supermarket_prod.bronze;

CREATE SCHEMA IF NOT EXISTS
    dbw_supermarket_prod.silver;

CREATE SCHEMA IF NOT EXISTS
    dbw_supermarket_prod.gold;
