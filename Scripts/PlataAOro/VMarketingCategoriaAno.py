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

dim_clientes_parquet = "/workspaces/MaquinaPrueba/Plata/DimCliente"
dim_clientes_df = sc.read.parquet(dim_clientes_parquet)

fact_ventas_parquet = "/workspaces/MaquinaPrueba/Plata/FactVentas"
fact_ventas_df = sc.read.parquet(fact_ventas_parquet)

#tomamos la información que necesitamos.
fact_ventas_df = fact_ventas_df.select \
( \
    fact_ventas_df.TotalLinea, \
    fact_ventas_df.ProductID,
    fact_ventas_df.CustomerID, \
    fact_ventas_df.FechaPedido \
)

#primer join, fact_ventas con Fecha
fact_ventas_df = fact_ventas_df.join(dim_fecha_df,fact_ventas_df.FechaPedido == dim_fecha_df.dateInt)

columns_to_drop = ['FechaPedido','dateInt','CalendarDate','NombreDia','DiaDeLaSemana','DiaDelMes','Dia','Semana','Trimestre','EsUltimoDiaDeMes']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)

fact_ventas_df.createOrReplaceTempView("FactVentas")
fact_ventas_df = sc.sql("SELECT ProductID,CustomerID,Ano,NombreMes,Mes,SUM(TotalLinea) as TotalProducto \
                         FROM FactVentas \
                         GROUP BY ProductID,CustomerID,Ano,NombreMes,Mes ")

#Ahora haremos el join con los clientes para mostar el nombre del cliente, el ordenamiento lo dejaremos para la herrmienta
#de visionado del Gerente

fact_ventas_df = fact_ventas_df.join(dim_productos_df,fact_ventas_df.ProductID == dim_productos_df.ProductID)
columns_to_drop = ['PrecioCatalogo','Tamano','Peso','Clase','Estilo','Color','EnProd','ProductID']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)
fact_ventas_df.show(100)

fact_ventas_df = fact_ventas_df.join(dim_clientes_df,fact_ventas_df.CustomerID == dim_clientes_df.CustomerID)
columns_to_drop = ['CustomerID','TerritoryID','PersonType','IDCliente']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)
fact_ventas_df.show()

fact_ventas_df.createOrReplaceTempView("FactVentas")
fact_ventas_df = sc.sql("SELECT Nombre,NombreCategoria,Ano,NombreMes,Mes,SUM(TotalProducto) as Total \
                         FROM FactVentas \
                         GROUP BY Nombre,Ano,NombreMes,Mes,NombreCategoria")

fact_ventas_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Oro/VMarketingCategoriaAno")
fact_ventas_df.show(100)