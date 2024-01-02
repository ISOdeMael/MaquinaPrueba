from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

salesHeader_csv = "/workspaces/MaquinaPrueba/DatosBase/SalesHeader.csv"

salesHeader_df = sc.read.option("delimiter", ";").option("header",True).option("inferSchema","True").csv(salesHeader_csv)

salesHeader_df = salesHeader_df.select \
 ( \
    salesHeader_df.SalesOrderID, \
    salesHeader_df.OrderDate.cast(DateType()).alias("FechaPedido"), \
    salesHeader_df.DueDate.cast(DateType()).alias("FechaVenc"), \
    salesHeader_df.ShipDate.cast(DateType()).alias("FechaEnv"), \
    salesHeader_df.Status, \
    salesHeader_df.CustomerID, \
    salesHeader_df.TerritoryID, \
    salesHeader_df.ShipMethodID, \
    salesHeader_df.ModifiedDate.cast(DateType()).alias("FechaMod")
 )

salesHeader_df = salesHeader_df \
        .fillna({ \
            "CustomerID" : -1, \
            "TerritoryID" : -1
         })
salesHeader_df.printSchema()
salesHeader_df.show(1)

salesHeader_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/SalesHeader")

