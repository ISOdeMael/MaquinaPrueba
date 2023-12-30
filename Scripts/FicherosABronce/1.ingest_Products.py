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
        StructField("ListPrice", StringType()),
        StructField("Size", StringType()),
        StructField("SizeUnitMeasureCode",StringType()),
        StructField("WeightUnitMeasureCode",StringType()),
        StructField("Weight",StringType()),
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

#products_df.printSchema()
#products_df.show(10)

#seleccionar ñp que necesitamos.

products_selected_df = products_df.select \
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
     col("ProductSubCategoryID").alias("SubCatID"), \
     col("SellStartDate").alias("InicioVenta_"), \
     col("SellEndDate").alias("FinalVenta_") \
 )

products_cleanNULL_df = products_selected_df \
        .fillna({ \
            "Color_" : "Trans", \
            "Tamano" : "ST", \
            "Peso" : 0, \
            "Clase" : "SC", \
            "Estilo" : "SE"
         })
products_df = products_cleanNULL_df.withColumn("EnProd",when(products_cleanNULL_df.EnProduccion == 1,True).otherwise(False))

products_df = products_df.withColumn("Color" \
              ,when(ucase(products_df.Color_) == "ROJO","Red") \
              .when(ucase(products_df.Color_) == "PLATA","Silver") \
              .when(ucase(products_df.Color_) == "AMBAR","Yellow") \
              .otherwise(products_df.Color_)  )
products_df = products_df.withColumn("InicioVenta" \
              ,to_date(products_df.InicioVenta_))
              
columns_to_drop = ['Color_', 'EnProduccion', 'InicioVenta_']
products_df = products_df.drop(*columns_to_drop)
products_df.show(200)

products_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/Products")