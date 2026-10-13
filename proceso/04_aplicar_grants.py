# Databricks notebook source

# CONFIGURACIÓN DE SEGURIDAD - UNITY CATALOG

dbutils.widgets.text("catalogo", "dbw_supermarket_dev")
dbutils.widgets.text("grupo", "supermarket_lectores")

catalogo = dbutils.widgets.get("catalogo")
grupo = dbutils.widgets.get("grupo")

# Validar identificadores
import re

for valor in [catalogo, grupo]:
    if not re.fullmatch(r"[A-Za-z0-9_]+", valor):
        raise ValueError("Nombre de catálogo o grupo inválido")

# 1. Permitir acceso al catálogo
spark.sql(f"""
    GRANT USE CATALOG
    ON CATALOG `{catalogo}`
    TO `{grupo}`
""")

print("OK - Acceso al catálogo")

# 2. Permitir acceso al esquema Gold
spark.sql(f"""
    GRANT USE SCHEMA
    ON SCHEMA `{catalogo}`.`gold`
    TO `{grupo}`
""")

print("OK - Acceso al esquema Gold")

# 3. Permitir consultar las tablas Gold
spark.sql(f"""
    GRANT SELECT
    ON SCHEMA `{catalogo}`.`gold`
    TO `{grupo}`
""")

print("OK - Permiso SELECT sobre Gold")

# 4. Verificar permisos
display(
    spark.sql(f"""
        SHOW GRANTS
        ON SCHEMA `{catalogo}`.`gold`
    """)
)

print("CONFIGURACIÓN DE SEGURIDAD COMPLETADA")
