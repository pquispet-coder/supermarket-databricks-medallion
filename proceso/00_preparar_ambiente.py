# Databricks notebook source
dbutils.widgets.text(
    "catalogo",
    "dbw_supermarket_dev"
)

catalogo = dbutils.widgets.get("catalogo")

# Validar que el catálogo exista
spark.sql(f"USE CATALOG `{catalogo}`")

print(f"Catálogo seleccionado: {catalogo}")

# Crear esquemas necesarios
esquemas = ["bronze", "silver", "gold"]

for esquema in esquemas:

    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS
        `{catalogo}`.`{esquema}`
    """)

    print(f"OK - Esquema {catalogo}.{esquema}")

print("PREPARACIÓN DEL AMBIENTE COMPLETADA")
