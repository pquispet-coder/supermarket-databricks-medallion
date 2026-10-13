# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# PARÁMETROS DEL ENTORNO

dbutils.widgets.text("catalogo", "dbw_supermarket_dev")
dbutils.widgets.text("storage_account", "stdatabricksfinal01")
dbutils.widgets.text("contenedor_raw", "raw")

catalogo = dbutils.widgets.get("catalogo")
storage_account = dbutils.widgets.get("storage_account")
contenedor_raw = dbutils.widgets.get("contenedor_raw")

ruta_raw = (
    f"abfss://{contenedor_raw}@"
    f"{storage_account}.dfs.core.windows.net/"
)

# ESTRUCTURA DE LOS ARCHIVOS

archivos = {
    "aisles": [
        "aisle_id", "aisle"
    ],
    "departments": [
        "department_id", "department"
    ],
    "products": [
        "product_id", "product_name",
        "aisle_id", "department_id",
        "_columna_extra"
    ],
    "orders": [
        "order_id", "user_id", "eval_set",
        "order_number", "order_dow",
        "order_hour_of_day",
        "days_since_prior_order"
    ],
    "order_products__prior": [
        "order_id", "product_id",
        "add_to_cart_order", "reordered"
    ],
    "order_products__train": [
        "order_id", "product_id",
        "add_to_cart_order", "reordered"
    ]
}

# 3. INGESTA A BRONZE

for nombre, columnas in archivos.items():

    esquema = StructType([
        StructField(columna, StringType(), True)
        for columna in columnas
    ])

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("mode", "PERMISSIVE")
        .schema(esquema)
        .load(f"{ruta_raw}{nombre}.csv")
    )

    # El archivo products tiene un separador
    # adicional al final de cada registro
    if nombre == "products":
        df = df.drop("_columna_extra")

    # Metadatos de trazabilidad
    df = (
        df
        .withColumn(
            "fecha_ingesta",
            F.current_timestamp()
        )
        .withColumn(
            "archivo_origen",
            F.lit(f"{nombre}.csv")
        )
    )

    tabla = f"{catalogo}.bronze.{nombre}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla)
    )

    print(f"OK - {tabla}")

print("INGESTA BRONZE COMPLETADA")
