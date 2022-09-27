# Databricks notebook source
# upper function, col etc, lit, concat etc.. are built in functions
# UDF - User Defined Function , reuse in data frame and spark sql

# UDF - User Defined Functions, custom functions written in scala,java, python 
# usedin spakr sql, spark dataframe
# python - slow
# scala, java - fast

# COMMAND ----------

# Databricks notebook source
# UDF - User Defined Functions
# useful to extend spark sql functions with custom code

# UDF can be lambda or even normal def function

power = lambda n : n * n

from pyspark.sql.functions import udf 
from pyspark.sql.types import LongType
# create udf with return type
powerUdf = udf(power, LongType())

# we must register udf in spark session
# udf too private within spark session, udf registered in spark session not avaialble in another spark session
# "power" is udf function name, can be used sql
spark.udf.register("power", powerUdf)

# COMMAND ----------

import pyspark.sql.functions as F
spark.createDataFrame(data=[(1,),(2,),(3,),(4,),(5,)], schema=['n'])\
     .withColumn("powern", powerUdf(F.col('n')) )\
     .show()

# COMMAND ----------

# How to udf in Spark SQL
# use the name we register the udf "power" in sql
spark.sql("SELECT power(5)").show()

# COMMAND ----------

# Databricks notebook source
# Databricks notebook source
orders = [ 
          # (product_id, product_name, brand_id, price, qty, discount, taxp)  
         (1, 'iPhone', 100, 1000, 2, 5, 18),
         (2, 'Galaxy', 200, 800, 1, 8, 22),

]
 


orderDf = spark.createDataFrame(data=orders, schema=["product_id", "product_name", "brand_id", "price", "qty", "discount", "taxp"])
orderDf.show()

# COMMAND ----------

# UDF to calculate amount
# amount = ( price * qty ) * apply discount * taxp

def calculateAmount(price, qty, discount, taxp):
    a = price * qty
    a = a - (a * discount/100) # discounted price
    amount = a + a * taxp / 100 # with tax
    print ("amount is" , amount) 
    return amount

print(calculateAmount(1000, 2, 5, 18))

# COMMAND ----------

from pyspark.sql.types import DoubleType
# udf function, DoubleType is return type of the data
calculate = udf(calculateAmount, DoubleType())
# "calculate" is used in spark sql SELECT calculate(...)
# the udf calculate available only on spark session, not in other spark session
# there is no global udf to use with all spark session, we need register with all spark session to use it
spark.udf.register("calculate", calculate)

# COMMAND ----------

# use udf in data frame
from pyspark.sql.functions import col
df = orderDf.withColumn("amount", calculate( col("price"), col("qty"), col("discount"), col("taxp")))
df.printSchema()
df.show()

# COMMAND ----------

# create a temp table/view
orderDf.createOrReplaceTempView("orders")

# COMMAND ----------

## now apply udf on spark sql

df = spark.sql("SELECT *, calculate(price, qty, discount, taxp) as amount from orders")
df.printSchema()
df.show()

# COMMAND ----------

