from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadXML").getOrCreate()

products_json = "/workspaces/MaquinaPrueba/DatosBase/Products.json"

products_schema = StructType(fields=[
        StructField("ProductID", IntegerType(),False),
        StructField("Name", StringType()),
        StructField("ProductNumber", StringType()),
        StructField("MakeFlag", StringType()),
        StructField("FinishedGoodsFlag", StringType()),
        StructField("Color", StringType()),
        StructField("SafetyStockLevel", IntegerType()),
        StructField("ReorderPoint", IntegerType()),
        StructField("StandardCost", DecimalType(8,2)),
        StructField("ListPrice", DecimalType(8,2)),
        StructField("Size", StringType()),
        StructField("SizeUnitMeasureCode",StringType()),
        StructField("WeightUnitMeasureCode",StringType()),
        StructField("Weight",DecimalType(8,2)),
        StructField("DaysToManufacture",StringType()),
        StructField("ProductLine",StringType()),
        StructField("Class",StringType()),
        StructField("Style",StringType()),
        StructField("ProductSubcategoryID",StringType()),
        StructField("ProductModelID",StringType()),
        StructField("SellStartDate",StringType()),
        StructField("SellEndDate",StringType()), 
        StructField("DiscontinuedDate",StringType()),
        StructField("rowguid",StringType()),
        StructField("ModifiedDate",StringType())
])

products_df = spark.read \
.schema(products_schema) \
.option("multiLine",True) \
.json(products_json)

products_df = products_df.select \
 ( \
     col("ProductID"), \
     col("Name").alias("NombreProducto"), \
     col("MakeFlag").alias("EnProduccion"), \
     col("Color").alias("Color_"), \
     col("ListPrice").alias("PrecioCatalogo"), \
     col("Size").alias("Tamano"), \
     col("Weight").alias("Peso"), \
     col("Class").alias("Clase"), \
     col("Style").alias("Estilo"), \
     col("ProductSubCategoryID").alias("SubCatID") \
 )

products_df = products_df \
        .fillna({ \
            "Color_" : "Sin Color", \
            "Tamano" : "Sin Tamaño", \
            "Peso" : 0, \
            "Clase" : "Sin Clase", \
            "Estilo" : "Sin Estilo", \
            "SubCatID" : -1 \
         })

products_df = products_df.withColumn("Color" \
              ,when(ucase(products_df.Color_) == "ROJO","Red") \
              .when(ucase(products_df.Color_) == "PLATA","Silver") \
              .when(ucase(products_df.Color_) == "AMBAR","Yellow") \
              .otherwise(products_df.Color_)  )
columns_to_drop = ['Color_']
products_df = products_df.drop(*columns_to_drop)

columnas = ['ProductID', 'NombreProducto','PrecioCatalogo','Tamano','Peso','Clase','Estilo','SubCatID','Color','EnProduccion']
newRow = spark.createDataFrame( \
    [ \
    (-1, "Producto No Informado ",0.0,"Sin Tamaño",0,"Sin Clase","Sin Estilo",-1,"Sin Color","0"), \
    (-2, "Producto No Encontrado",0.0,"Sin Tamaño",0,"Sin Clase","Sin Estilo",-1,"Sin Color","0") \
    ], columnas)
products_df = products_df.union(newRow)
products_df = products_df.withColumn("EnProd",when(products_df.EnProduccion == 1,True).otherwise(False))
columns_to_drop = ['EnProduccion_']
products_df = products_df.drop(*columns_to_drop)
products_df.printSchema()
products_df.show(600)

products_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/Products")