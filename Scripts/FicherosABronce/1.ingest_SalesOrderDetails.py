from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

salesDetail_csv = "/workspaces/MaquinaPrueba/DatosBase/SalesDetail.csv"

salesDetail_df = sc.read.option("delimiter", ";").option("header",True).option("inferSchema","True").csv(salesDetail_csv)

salesDetail_df = salesDetail_df.select \
 ( \
    salesDetail_df.SalesOrderID, \
    salesDetail_df.SalesOrderDetailID, \
    salesDetail_df.OrderQty.alias("Cantidad"), \
    salesDetail_df.ProductID, \
    salesDetail_df.UnitPrice, \
    salesDetail_df.UnitPriceDiscount, \
    salesDetail_df.ModifiedDate.cast(DateType()).alias("FechaMod")
 )


salesDetail_df = salesDetail_df \
        .fillna({ \
            "ProductID" : -1 
         })

salesDetail_df = salesDetail_df.withColumn("Precio_", regexp_replace(col("UnitPrice"), ",", "."))
salesDetail_df = salesDetail_df.withColumn("Precio", salesDetail_df.Precio_.cast(DecimalType(8,3)))
salesDetail_df = salesDetail_df.withColumn("TotalLinea",salesDetail_df.Precio*salesDetail_df.Cantidad)
salesDetail_df = salesDetail_df.select \
( \
    salesDetail_df.SalesOrderID, \
    salesDetail_df.SalesOrderDetailID, \
    salesDetail_df.Cantidad, \
    salesDetail_df.ProductID, \
    salesDetail_df.Precio, \
    salesDetail_df.TotalLinea, \
    salesDetail_df.UnitPriceDiscount, \
    salesDetail_df.FechaMod 
)
salesDetail_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/SalesDetail")
salesDetail_df.show()


