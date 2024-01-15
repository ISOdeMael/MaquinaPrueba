from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadCSV").getOrCreate()
sc = SQLContext(spark)

product_parquet = "/workspaces/MaquinaPrueba/Bronce/Products"
product_df = sc.read.parquet(product_parquet)
subcategory_parquet = "/workspaces/MaquinaPrueba/Bronce/SubCategory"
subcategory_df= sc.read.parquet(subcategory_parquet)
category_parquet = "/workspaces/MaquinaPrueba/Bronce/Category"
category_df = sc.read.parquet(category_parquet)

productWithSubcategory_df = product_df.join(subcategory_df, product_df.SubCatID == subcategory_df.SubCategoryID, "leftouter")
productWithSubcategory_df.createOrReplaceTempView("productWithSub")
#Vamos a ver los que no coinciden con subcategoria, para que no se pierda la información dde estos productos
#vamos a poner como código de suncategoria la -2
productWithSubcategory_df = sc.sql(" \
SELECT ProductID, NombreProducto, PrecioCatalogo,Tamano,Peso,Clase,Estilo, \
    CASE WHEN SubCategoryID IS NULL THEN '-2' ELSE SubCatID END AS SubCatID, \
    Color,EnProd, \
    CASE WHEN SubCategorYID IS NULL THEN '-2' ELSE CategoryID END AS CategoryID, \
    CASE WHEN SubCategoryID IS NULL THEN 'SubCategoria No Encontrada' ELSE NombreSubCategoria END AS NombreSubCategoria \
FROM productWithSub ")
complete_df = productWithSubcategory_df.join(category_df, productWithSubcategory_df.CategoryID == category_df.CategoryID, "leftouter")
columns_to_drop = ['SubCatID','CategoryID']
complete_df = complete_df.drop(*columns_to_drop)
complete_df.write.mode("overwrite").parquet("/workspaces/MaquinaPrueba/Plata/DimProducto")
complete_df.show(100)
