# Databricks notebook source
# spark , a variable already initialized with spark session
# spark session is an entry point for spark sql/data frame
# in any spark applications, there is ONLY ONE Spark Context, 
# there can be MANY SPARK SESSIONS [ISOLATION etc]

spark

# COMMAND ----------

# data type is inferred by spark itself
# infer done by spark driver [infer is not good for performance]
df = spark.createDataFrame(data=[(10, 'F'), (11, 'F'), (12, 'M')], schema=["age","gender"])
df.printSchema() # schema tree printed
df.show() # ASCII Table

# COMMAND ----------

# databricks has display function [not open source spark]
# display function can print html5 output, charts, tablets, etc
display(df)

# COMMAND ----------

sc

# COMMAND ----------

spark

# COMMAND ----------

