from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Composicion").getOrCreate()
sc = SQLContext(spark)

header_parquet = "/workspaces/MaquinaPrueba/Bronce/SalesHeader"
header_df = sc.read.parquet(header_parquet)
detail_parquet = "/workspaces/MaquinaPrueba/Bronce/SalesDetail"
detail_df= sc.read.parquet(detail_parquet)
#header_df.show(1000)
#detail_df.show(1000)
complete_df = detail_df.join(header_df, detail_df.SalesOrderID == header_df.OrderID, "leftouter")
complete_df.show(10)
#complete_df.filter(complete_df.Status.isNull()).show(10)
columns_to_drop = ['OrderID','FechaMod']
complete_df = complete_df.drop(*columns_to_drop)
complete_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Plata/FactVentas")
complete_df.show(10)
