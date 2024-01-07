from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

customer_json = "/workspaces/MaquinaPrueba/DatosBase/Customers.csv"

customer_df = sc.read.load(customer_json,format='com.databricks.spark.csv',header='true',inferSchema='true').cache()


customer_df = customer_df.select \
 ( \
    customer_df.CustomerID, \
    customer_df.PersonID.cast(IntegerType()).alias('PersonID'), \
    customer_df.TerritoryID \
 )

customer_df = customer_df \
        .fillna({ \
            "PersonID" : -1, \
            "TerritoryID" : -1
         })

columnas = ['CustomerID', 'PersonID', 'TerritoryID']
newRow = spark.createDataFrame([(-1, -1, -1),(-2, -2, -2,)], columnas)
customer_df = customer_df.union(newRow)

customer_df.show(10000)
customer_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/Customers")

