-- SEGURIDAD - UNITY CATALOG PROD

-- Grupo: supermarket_lectores
-- Acceso de solo lectura a la capa Gold

-- Acceso al catálogo
GRANT USE CATALOG
ON CATALOG dbw_supermarket_prod
TO `supermarket_lectores`;

-- Acceso al esquema Gold
GRANT USE SCHEMA
ON SCHEMA dbw_supermarket_prod.gold
TO `supermarket_lectores`;

-- Consulta de tablas Gold
GRANT SELECT
ON SCHEMA dbw_supermarket_prod.gold
TO `supermarket_lectores`;

-- Verificación de permisos
SHOW GRANTS ON SCHEMA dbw_supermarket_prod.gold;
