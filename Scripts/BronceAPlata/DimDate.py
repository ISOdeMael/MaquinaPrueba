from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("fechas").getOrCreate()
beginDate = '2000-01-01'
endDate = '2050-12-31'
(
  spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate")
    .createOrReplaceTempView('dates')
)
dim_fecha_df = spark.sql(" select  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as dateInt, \
  CalendarDate, \
  year(calendarDate) AS Ano, \
    case month(calendarDate)\
      when 1 then 'Enero' \
      when 2 then 'Febrero' \
      when 3 then 'Marzo' \
      when 4 then 'Abril' \
      when 5 then 'Mayo' \
      when 6 then 'Junio' \
      when 7 then 'Julio' \
      when 8 then 'Agosto' \
      when 9 then 'Septiembre' \
      when 10 then 'Octubre' \
      when 11 then 'Noviembre' \
      when 12 then 'Diciembre' \
  end as NombreMes, \
  month(calendarDate) as Mes, \
  case weekday(calendarDate) \
      when 6 then 'Domingo' \
      when 0 then 'Lunes' \
      when 1 then 'Martes' \
      when 2 then 'Miercoles' \
      when 3 then 'Jueves' \
      when 4 then 'Viernes' \
      when 5 then 'Sabado' \
  end as NombreDia, \
  dayofweek(calendarDate) AS DiaDeLaSemana, \
  dayofmonth(calendarDate) as DiaDelMes, \
  case when calendarDate = last_day(calendarDate) then 'S' else 'N' end as EsUltimoDiaDeMes, \
  dayofyear(calendarDate) as Dia, \
  weekofyear(calendarDate) as Semana, \
  quarter(calendarDate) as Trimestre \
from \
  dates \
order by \
  calendarDate  ")

dim_fecha_df.show(12)
dim_fecha_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Plata/DimFecha")
