# Databricks notebook source

from pyspark.sql import functions as F

# Parámetro para DEV y PROD
dbutils.widgets.text("catalogo", "dbw_supermarket_dev")
catalogo = dbutils.widgets.get("catalogo")

# Leer Bronze
aisles = spark.table(f"{catalogo}.bronze.aisles")
departments = spark.table(f"{catalogo}.bronze.departments")
products = spark.table(f"{catalogo}.bronze.products")
orders = spark.table(f"{catalogo}.bronze.orders")

# Limpiar pasillos
aisles = (
    aisles.select(
        F.col("aisle_id").cast("int"),
        F.trim("aisle").alias("aisle")
    )
    .dropna(subset=["aisle_id"])
    .dropDuplicates(["aisle_id"])
)

# Limpiar departamentos
departments = (
    departments.select(
        F.col("department_id").cast("int"),
        F.trim("department").alias("department")
    )
    .dropna(subset=["department_id"])
    .dropDuplicates(["department_id"])
)

# Limpiar productos
products = (
    products.select(
        F.col("product_id").cast("int"),
        F.trim("product_name").alias("product_name"),
        F.col("aisle_id").cast("int"),
        F.col("department_id").cast("int")
    )
    .dropna(subset=["product_id"])
    .dropDuplicates(["product_id"])
)

# Optimizar relaciones con tablas pequeñas
productos_silver = (
    products
    .join(F.broadcast(aisles), "aisle_id", "left")
    .join(F.broadcast(departments), "department_id", "left")
    .withColumn("fecha_procesamiento", F.current_timestamp())
)

# Limpiar pedidos
pedidos_silver = (
    orders.select(
        F.col("order_id").cast("long"),
        F.col("user_id").cast("long"),
        F.trim("eval_set").alias("eval_set"),
        F.col("order_number").cast("int"),
        F.col("order_dow").cast("int"),
        F.col("order_hour_of_day").cast("int"),
        F.col("days_since_prior_order").cast("double")
    )
    .dropna(subset=["order_id", "user_id"])
    .dropDuplicates(["order_id"])
    .filter(F.col("order_hour_of_day").between(0, 23))
    .filter(F.col("order_dow").between(0, 6))
    .withColumn("fecha_procesamiento", F.current_timestamp())
)

# Guardar Silver
for nombre, dataframe in [
    ("productos", productos_silver),
    ("pedidos", pedidos_silver)
]:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalogo}.silver.{nombre}")
    )

    print(f"OK - Silver {nombre}")

print("Primera transformación Silver completada")


# COMMAND ----------


print("Productos Silver:")
display(spark.table(f"{catalogo}.silver.productos").limit(10))

print("Pedidos Silver:")
display(spark.table(f"{catalogo}.silver.pedidos").limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

# LEER Y PREPARAR DETALLES BRONZE

def preparar_detalle(nombre, origen):
    return (
        spark.table(f"{catalogo}.bronze.{nombre}")
        .select(
            F.col("order_id").cast("long").alias("order_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("add_to_cart_order").cast("int").alias("add_to_cart_order"),
            F.col("reordered").cast("int").alias("reordered")
        )
        .withColumn("origen", F.lit(origen))
    )

prior = preparar_detalle("order_products__prior", "prior")
train = preparar_detalle("order_products__train", "train")

detalle = prior.unionByName(train)

# LEER TABLAS SILVER

pedidos = (
    spark.table(f"{catalogo}.silver.pedidos")
    .select(
        "order_id", "user_id", "eval_set",
        "order_number", "order_dow",
        "order_hour_of_day", "days_since_prior_order"
    )
)

productos = (
    spark.table(f"{catalogo}.silver.productos")
    .select(
        "product_id", "product_name",
        "aisle_id", "aisle",
        "department_id", "department"
    )
)

# VALIDACIÓN DE CALIDAD

cruce = (
    detalle.alias("d")
    .join(
        pedidos.alias("p"),
        F.col("d.order_id") == F.col("p.order_id"),
        "left"
    )
    .withColumn(
        "estado_calidad",
        F.when(
            F.col("d.order_id").isNull() |
            F.col("d.product_id").isNull(),
            "ID inválido"
        )
        .when(
            F.col("p.order_id").isNull(),
            "Sin pedido asociado"
        )
        .when(
            F.col("d.origen") != F.col("p.eval_set"),
            "Tipo inconsistente"
        )
        .when(
            F.col("d.reordered").isNull() |
            (~F.col("d.reordered").isin(0, 1)),
            "Indicador inválido"
        )
        .when(
            F.col("d.add_to_cart_order").isNull() |
            (F.col("d.add_to_cart_order") <= 0),
            "Posición inválida"
        )
        .otherwise("Correcto")
    )
)

# Guardar auditoría
auditoria = (
    cruce
    .groupBy(
        F.col("d.origen").alias("origen"),
        "estado_calidad"
    )
    .count()
)

(
    auditoria.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.silver.auditoria_relaciones")
)

print("OK - Auditoría de calidad")

# CONSOLIDAR COMPRAS VÁLIDAS

compras_validas = (
    cruce
    .filter(F.col("estado_calidad") == "Correcto")
    .select(
        F.col("d.order_id").alias("order_id"),
        F.col("d.product_id").alias("product_id"),
        F.col("d.add_to_cart_order").alias("add_to_cart_order"),
        F.col("d.reordered").alias("reordered"),
        F.col("d.origen").alias("origen"),
        F.col("p.user_id").alias("user_id"),
        F.col("p.order_number").alias("order_number"),
        F.col("p.order_dow").alias("order_dow"),
        F.col("p.order_hour_of_day").alias("order_hour_of_day"),
        F.col("p.days_since_prior_order").alias("days_since_prior_order")
    )
    .dropDuplicates(["order_id", "product_id"])
)

# Enriquecer con catálogo de productos
compras_silver = (
    compras_validas
    .join(F.broadcast(productos), "product_id", "left")
    .withColumn(
        "estado_recompra",
        F.when(F.col("reordered") == 1, "Recompra")
         .otherwise("Primera compra")
    )
    .withColumn(
        "fecha_procesamiento",
        F.current_timestamp()
    )
)

# GUARDAR SILVER

(
    compras_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalogo}.silver.compras")
)

print("OK - Silver compras")
print("TRANSFORMACIÓN SILVER COMPLETADA")

display(auditoria.orderBy("origen", "estado_calidad"))


# COMMAND ----------


from pyspark.sql import functions as F

# Tablas Silver
compras = spark.table(f"{catalogo}.silver.compras")
auditoria = spark.table(f"{catalogo}.silver.auditoria_relaciones")

# 1. Resultados de calidad
print("=== AUDITORÍA DE CALIDAD ===")
display(
    auditoria.orderBy("origen", "estado_calidad")
)

# 2. Validación de compras
print("=== VALIDACIÓN DE COMPRAS ===")

resumen = compras.agg(
    F.count("*").alias("total_registros"),
    F.countDistinct("order_id").alias("pedidos_unicos"),
    F.countDistinct("product_id").alias("productos_unicos"),
    F.countDistinct("user_id").alias("clientes_unicos"),
    F.sum(
        F.when(F.col("product_name").isNull(), 1)
         .otherwise(0)
    ).alias("productos_sin_descripcion"),
    F.sum(
        F.when(F.col("reordered") == 1, 1)
         .otherwise(0)
    ).alias("registros_recompra")
)

display(resumen)

# 3. Verificar duplicados
duplicados = (
    compras
    .groupBy("order_id", "product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Combinaciones pedido-producto duplicadas:", duplicados)
