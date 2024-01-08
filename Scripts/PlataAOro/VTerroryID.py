from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Ventas").getOrCreate()
sc = SQLContext(spark)

#vamos a cargar en tablas temporales y luego haremos SQL para extraer la información

dim_clientes_parquet =  "/workspaces/MaquinaPrueba/Plata/DimCliente"
dim_clientes_df = sc.read.parquet(dim_clientes_parquet)
#dim_clientes_df.show(2)

dim_fecha_parquet = "/workspaces/MaquinaPrueba/Plata/DimFecha"
dim_fecha_df = sc.read.parquet(dim_fecha_parquet)
#dim_fecha_df.show(2)

fact_ventas_parquet = "/workspaces/MaquinaPrueba/Plata/FactVentas"
fact_ventas_df = sc.read.parquet(fact_ventas_parquet)
#fact_ventas_df.show()

#tomamos la información que necesitemos.
fact_ventas_df = fact_ventas_df.select \
( \
    fact_ventas_df.TotalLinea, \
    fact_ventas_df.TerritoryID, \
    fact_ventas_df.FechaPedido \
)

#primer join, fact_ventas con Fecha
fact_ventas_df = fact_ventas_df.join(dim_fecha_df,fact_ventas_df.FechaPedido == dim_fecha_df.dateInt)

columns_to_drop = ['FechaPedido','dateInt','CalendarDate','NombreDia','DiaDeLaSemana','DiaDelMes','Dia','Semana','Trimestre','EsUltimoDiaDeMes']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)

#vamos a agrupar por código de Cliente
#fact_ventas_df.show(10)
fact_ventas_df.createOrReplaceTempView("FactVentas")
fact_ventas_df = sc.sql("SELECT TerritoryID,Ano,NombreMes,Mes,SUM(TotalLinea) as TotatCliente \
                         FROM FactVentas \
                         GROUP BY TerritoryID,Ano,NombreMes,Mes ")
fact_ventas_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Oro/VTerritoryID")
fact_ventas_df.show(100)