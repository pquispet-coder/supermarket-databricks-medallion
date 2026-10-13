# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# PARÁMETROS DEL ENTORNO

dbutils.widgets.text("catalogo", "dbw_supermarket_dev")
dbutils.widgets.text("source", "silver")
dbutils.widgets.text("sink", "gold")

catalogo = dbutils.widgets.get("catalogo")
source = dbutils.widgets.get("source")
sink = dbutils.widgets.get("sink")

# LECTURA Y VALIDACIÓN

compras = spark.table(
    f"{catalogo}.{source}.compras"
)

total_registros = compras.count()

if total_registros == 0:
    raise ValueError("La tabla Silver compras está vacía")

print(f"Registros Silver: {total_registros:,}")

# GOLD - PRODUCTOS POPULARES

gold_productos = (
    compras
    .groupBy(
        "product_id",
        "product_name",
        "department"
    )
    .agg(
        F.count("*").alias("total_compras"),
        F.sum("reordered").alias("total_recompras"),
        F.countDistinct("order_id").alias("pedidos_unicos"),
        F.countDistinct("user_id").alias("clientes_unicos")
    )
    .withColumn(
        "tasa_recompra",
        F.round(
            F.col("total_recompras") /
            F.col("total_compras") * 100, 2
        )
    )
    .withColumn(
        "ranking_popularidad",
        F.row_number().over(
            Window.orderBy(
                F.desc("total_compras"),
                F.asc("product_id")
            )
        )
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp()
    )
)

(
    gold_productos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalogo}.{sink}.productos_populares"
    )
)

print("OK - Gold productos_populares")

# GOLD - DEPARTAMENTOS

gold_departamentos = (
    compras
    .groupBy(
        "department_id",
        "department"
    )
    .agg(
        F.count("*").alias("total_productos_comprados"),
        F.countDistinct("order_id").alias("pedidos_unicos"),
        F.countDistinct("product_id").alias("productos_unicos"),
        F.sum("reordered").alias("total_recompras")
    )
    .withColumn(
        "tasa_recompra",
        F.round(
            F.col("total_recompras") /
            F.col("total_productos_comprados") * 100, 2
        )
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp()
    )
)

(
    gold_departamentos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalogo}.{sink}.departamentos"
    )
)

print("OK - Gold departamentos")

# GOLD - PEDIDOS POR HORA

# Primero obtener un registro por pedido
pedidos = (
    compras
    .groupBy("order_id", "order_hour_of_day")
    .agg(
        F.count("*").alias("articulos_por_pedido")
    )
)

total_pedidos = pedidos.count()

gold_horarios = (
    pedidos
    .groupBy("order_hour_of_day")
    .agg(
        F.count("*").alias("total_pedidos"),
        F.round(
            F.avg("articulos_por_pedido"), 2
        ).alias("promedio_articulos_pedido")
    )
    .withColumn(
        "porcentaje_pedidos",
        F.round(
            F.col("total_pedidos") /
            F.lit(total_pedidos) * 100, 2
        )
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp()
    )
)

(
    gold_horarios.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{catalogo}.{sink}.pedidos_por_hora"
    )
)

print("OK - Gold pedidos_por_hora")
print("CAPA GOLD COMPLETADA")


# COMMAND ----------


from pyspark.sql import functions as F

# Obtener totales reales de Silver
compras = spark.table(f"{catalogo}.{source}.compras")

control = compras.agg(
    F.count("*").alias("compras"),
    F.countDistinct("order_id").alias("pedidos")
).first()

# Obtener totales de Gold
productos = spark.table(
    f"{catalogo}.{sink}.productos_populares"
)
departamentos = spark.table(
    f"{catalogo}.{sink}.departamentos"
)
horarios = spark.table(
    f"{catalogo}.{sink}.pedidos_por_hora"
)

total_productos = productos.agg(
    F.sum("total_compras")
).first()[0]

total_departamentos = departamentos.agg(
    F.sum("total_productos_comprados")
).first()[0]

total_pedidos = horarios.agg(
    F.sum("total_pedidos")
).first()[0]

print("=== VALIDACIÓN GOLD ===")
print("Compras en Silver:", control["compras"])
print("Compras en Gold (productos):", total_productos)
print("Compras en Gold (departamentos):", total_departamentos)
print("Pedidos en Silver:", control["pedidos"])
print("Pedidos en Gold:", total_pedidos)

assert total_productos == control["compras"]
assert total_departamentos == control["compras"]
assert total_pedidos == control["pedidos"]

print("VALIDACIÓN GOLD CORRECTA")
