# Datasets

Para este proyecto utilicé el dataset [Supermarket Superstore Dataset Bundle](https://www.kaggle.com/datasets/amunsentom/supermarket-superstore-dataset-bundle), disponible en Kaggle, que contiene información sobre productos, departamentos y pedidos realizados en un supermercado.

De este conjunto seleccioné seis archivos CSV: `aisles`, `departments`, `products`, `orders`, `order_products__prior` y `order_products__train`. Estos archivos permiten relacionar los pedidos con los productos comprados y analizar aspectos como los productos más populares, las recompras y los horarios de compra.

Los archivos originales se almacenaron en Azure Data Lake Storage Gen2, dentro del contenedor `raw`. Para acceder a ellos desde Databricks utilicé una Managed Identity, sin necesidad de cargar los datos en DBFS o Volumes.

Durante el procesamiento se identificaron registros de compras que no tenían un pedido asociado en los archivos disponibles. Estos registros se excluyeron del análisis y se contabilizaron en una tabla de auditoría. Por ello, los resultados corresponden a las compras válidas que pudieron relacionarse con la información disponible.
