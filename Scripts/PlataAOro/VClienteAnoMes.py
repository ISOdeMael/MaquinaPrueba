from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Ventas").getOrCreate()
sc = SQLContext(spark)

#vamos a cargar en tablas temporales y luego haremos SQL para extraer la información

dim_clientes_parquet =  "/workspaces/MaquinaPrueba/Plata/DimCliente"
dim_clientes_df = sc.read.parquet(dim_clientes_parquet)

dim_fecha_parquet = "/workspaces/MaquinaPrueba/Plata/DimFecha"
dim_fecha_df = sc.read.parquet(dim_fecha_parquet)

fact_ventas_parquet = "/workspaces/MaquinaPrueba/Plata/FactVentas"
fact_ventas_df = sc.read.parquet(fact_ventas_parquet)

#tomamos la información que necesitemos.
fact_ventas_df = fact_ventas_df.select \
( \
    fact_ventas_df.TotalLinea, \
    fact_ventas_df.CustomerID, \
    fact_ventas_df.FechaPedido \
)

#primer join, fact_ventas con Fecha
fact_ventas_df = fact_ventas_df.join(dim_fecha_df,fact_ventas_df.FechaPedido == dim_fecha_df.dateInt)

columns_to_drop = ['FechaPedido','dateInt','CalendarDate','NombreDia','DiaDeLaSemana','DiaDelMes','Dia','Semana','Trimestre','EsUltimoDiaDeMes']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)

fact_ventas_df.createOrReplaceTempView("FactVentas")
fact_ventas_df = sc.sql("SELECT CustomerID,Ano,NombreMes,Mes,SUM(TotalLinea) as TotatCliente \
                         FROM FactVentas \
                         GROUP BY CustomerID,Ano,NombreMes,Mes ")

#Ahora haremos el join con los clientes para mostar el nombre del cliente, el ordenamiento lo dejaremos para la herrmienta
#de visionado del Gerente

fact_ventas_df = fact_ventas_df.join(dim_clientes_df,fact_ventas_df.CustomerID == dim_clientes_df.CustomerID)
columns_to_drop = ['CustomerID','TerritoryID','PersonType','IDCliente']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)
fact_ventas_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Oro/VClienteAnoMes")
fact_ventas_df.show(100)