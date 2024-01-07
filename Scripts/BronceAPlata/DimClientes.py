from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

customer_parquet = "/workspaces/MaquinaPrueba/Bronce/Customers"
customer_df = sc.read.parquet(customer_parquet)
person_parquet = "/workspaces/MaquinaPrueba/Bronce/Person"
person_df= sc.read.parquet(person_parquet)
person_df = person_df.withColumn("IDCliente",person_df.PersonID)

#customer_df.show(1000)
#person_df.show(1000)
customerWithPerson_df = customer_df.join(person_df, customer_df.PersonID == person_df.IDCliente, "leftouter")
columns_to_drop = ['PersonID']
complete_df = customerWithPerson_df.drop(*columns_to_drop)
complete_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Plata/DimCliente")
complete_df.show(100)
