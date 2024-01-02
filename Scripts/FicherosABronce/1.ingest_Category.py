from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadXML").getOrCreate()

Category_csv = "/workspaces/MaquinaPrueba/DatosBase/ProductsCategory.csv"


Category_df = spark.read.option("delimiter", ";").csv(Category_csv)

#Category_df.printSchema()
#Category_df.show(10)

#seleccionar lo que necesitamos.

Category_df = Category_df.select \
 ( \
     col("_c0").alias("CategoryID"), \
     col("_c1").alias("NombreCategoria")
 )

columnas = ['CategoryID', 'NombreCategoria']
newRow = spark.createDataFrame([(-1, "Categoria No Informada")], columnas)
Category_df = Category_df.union(newRow)
newRow = spark.createDataFrame([(-2, "Categoria No Encontrada")], columnas)
Category_df = Category_df.union(newRow)

Category_df.show(100)

Category_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/Category")