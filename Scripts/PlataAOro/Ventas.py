from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Ventas").getOrCreate()
sc = SQLContext(spark)

#vamos a cargar en tablas temporales y luego haremos SQL para extraer la información

dim_clientes_parquet =  "/workspaces/MaquinaPrueba/Plata/DimCliente"
dim_clientes_df = sc.read(dim_clientes_parquet)

fact_ventas_parquet = "//workspaces/MaquinaPrueba/Plata/FactVentas"
fact_ventas_df = sc.read(fact_ventas_parquet)

dim_fecha = "/workspaces/MaquinaPrueba/Plata/DimFecha"


#header_df.show(1000)
#detail_df.show(1000)
complete_df = detail_df.join(header_df, detail_df.SalesOrderID == header_df.OrderID, "leftouter")
complete_df.show(10)
#complete_df.filter(complete_df.Status.isNull()).show(10)
columns_to_drop = ['OrderID','FechaMod']
complete_df = complete_df.drop(*columns_to_drop)
complete_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Plata/FactVentas")
complete_df.show(10)
