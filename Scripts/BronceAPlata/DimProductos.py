from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

product_parquet = "/workspaces/MaquinaPrueba/Bronce/Products"
product_df = sc.read.parquet(product_parquet)
subcategory_parquet = "/workspaces/MaquinaPrueba/Bronce/SubCategory"
subcategory_df= sc.read.parquet(subcategory_parquet)

product_df.show(20)
subcategory_df.show(20)


