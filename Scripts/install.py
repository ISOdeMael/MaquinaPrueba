from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
from pyspark.sql import Row
data=(
  Row(1,"Muhammad",22),
  Row(2,"Abdullah",24),
  Row(3,"Ahmed",44),
  Row(4,"John",55)
)
data=spark.createDataFrame(data)
data.show()

# data.write.parquet("prueba")


