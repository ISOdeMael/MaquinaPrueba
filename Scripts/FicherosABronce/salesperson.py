from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

sales_person_json = "/workspaces/MaquinaPrueba/DatosBase/Customers.csv"

sales_person_df = sc.read.load(sales_person_json,format='com.databricks.spark.csv',header='true',inferSchema='true').cache()


sales_person_df = sales_person_df.select \
 ( \
    sales_person_df.BusinessEntityID.cast(IntegerType()).alias('PersonID'), \
    sales_person_df.TerritoryID, \
    sales_person_df.SalesQuota, \
    sales_person_df.Bonus, \
    sales_person_df.CommissionPct, \
    sales_person_df.ModifiedDate.cast(DateType()).alias("FechaMod") 
 )

sales_person_df = sales_person_df.withColumn("FechaMod",( \
    year(sales_person_df.FechaMod)*10000 + \
    month(sales_person_df.FechaMod)*100 + \
    day(sales_person_df.FechaMod)))

sales_person_df = customer_df \
        .fillna({ \
            "PersonID" : -1, \
            "TerritoryID" : -1
         })

columnas = ['PersonID', 'TerritoryID', 'SalesQuota', 'Bonus', 'CommissionPct', 'ModifiedDate']
newRow = spark.createDataFrame([(-1, -1, 0, 0, 0, 19001231),(-2, -2, 0, 0, 0, 19001231)], columnas)
sales_person_df = sales_person_df.union(newRow)

sales_person_df.show(10000)
sales_person_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Bronce/SalesPerson")
