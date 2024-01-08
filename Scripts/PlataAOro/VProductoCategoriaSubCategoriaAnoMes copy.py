from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Ventas").getOrCreate()
sc = SQLContext(spark)

#vamos a cargar en tablas temporales y luego haremos SQL para extraer la información

dim_productos_parquet =  "/workspaces/MaquinaPrueba/Plata/DimProducto"
dim_productos_df = sc.read.parquet(dim_productos_parquet)

dim_fecha_parquet = "/workspaces/MaquinaPrueba/Plata/DimFecha"
dim_fecha_df = sc.read.parquet(dim_fecha_parquet)

fact_ventas_parquet = "/workspaces/MaquinaPrueba/Plata/FactVentas"
fact_ventas_df = sc.read.parquet(fact_ventas_parquet)

#tomamos la información que necesitemos.
fact_ventas_df = fact_ventas_df.select \
( \
    fact_ventas_df.TotalLinea, \
    fact_ventas_df.ProductID, \
    fact_ventas_df.FechaPedido \
)

#primer join, fact_ventas con Fecha
fact_ventas_df = fact_ventas_df.join(dim_fecha_df,fact_ventas_df.FechaPedido == dim_fecha_df.dateInt)

columns_to_drop = ['FechaPedido','dateInt','CalendarDate','NombreDia','DiaDeLaSemana','DiaDelMes','Dia','Semana','Trimestre','EsUltimoDiaDeMes']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)

fact_ventas_df.createOrReplaceTempView("FactVentas")
fact_ventas_df = sc.sql("SELECT ProductID,Ano,NombreMes,Mes,SUM(TotalLinea) as TotalProducto \
                         FROM FactVentas \
                         GROUP BY ProductID,Ano,NombreMes,Mes ")

#Ahora haremos el join con los clientes para mostar el nombre del cliente, el ordenamiento lo dejaremos para la herrmienta
#de visionado del Gerente

fact_ventas_df = fact_ventas_df.join(dim_productos_df,fact_ventas_df.ProductID == dim_productos_df.ProductID)
columns_to_drop = ['PrecioCatalogo','Tamano','Peso','Clase','Estilo','Color','EnProd','ProductID']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)
fact_ventas_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Oro/VProductoAnoMes")
fact_ventas_df.show(100)