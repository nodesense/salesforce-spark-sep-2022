# Databricks notebook source
# Databricks notebook source
# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), #   no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]



productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
productDf.show()

brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])

brandDf.show()

store = [
    #(store_id, store_name)
    (1000, "Poorvika"),
    (2000, "Sangeetha"),
    (4000, "Amazon"),
    (5000, "FlipKart"), 
]
 
storeDf = spark.createDataFrame(data=store, schema=["store_id", "store_name"])
storeDf.show()

# COMMAND ----------

# in any spark application, there will be ONLY ONE spark context
# and as many spark sessions allowed
# SPARK SESSIONS provides isolation against temp views, User Defined Function

spark2 = spark.newSession()

# COMMAND ----------

# now we have TWO Spark session, both are indepdent, work on same spark context
spark
spark2

spark == spark2 # false 
spark.sparkContext == spark2.sparkContext # True


# COMMAND ----------

# TEMP TABLE, TEMP VIEWS ARE SAME, they are temporary, when app complete the execution, they will be deleted
# no data is peristed for temp table/view, temp table or temp views alias name
# temp table doesn't have database
# we created productDf using spark.createDataFrame
# create product temp table in spark session
# products is temp view, private to spark session, means we cannot access from spark 2
productDf.createOrReplaceTempView("products")

# COMMAND ----------

df = spark.sql ("SHOW TABLES")
df.show()

# COMMAND ----------

# we create temp table from dataFrame createOrReplaceTempView
# then spark.sql returns data frame
# Run SQL directory, sql returns dataframe
# products temp table created inside spark session, accessible only within spark session
df = spark.sql ("SELECT * FROM products")
df.printSchema()
df.show()

# COMMAND ----------

# now access Products table with spark2 but it will FAIL
spark2.sql("SHOW TABLES").show()

# COMMAND ----------

# FAIL in spark2 session
spark2.sql("SELECT * FROM products").show () # FAIL Table or view not found

# COMMAND ----------

# CREATE DATAFRAME in spark2 session
brandDf2 = spark2.createDataFrame(data=brands, schema=["brand_id", "brand_name"])

brandDf2.show()

brandDf2.createOrReplaceTempView("brands")
# now brands temp view available in spark2 session, not in spark session



# COMMAND ----------

spark2.sql("SHOW TABLES").show()

# COMMAND ----------

# try accessing brands table from spark session will FAIL

spark.sql("SHOW TABLES").show()

# FAIL
spark.sql("SELECT * FROM brands").show() # FAIL Table or View not found

# COMMAND ----------

# global temp view is created in a global_temp database, [not persisted, temporary db]
# tables are created inside global_temp.stores
storeDf.createOrReplaceGlobalTempView("stores")

# COMMAND ----------

spark.sql("SHOW TABLES").show() # doesnpt show stores, shows from default

# COMMAND ----------

spark2.sql("SHOW TABLES").show() # doesnpt show stores, shows from default

# COMMAND ----------

spark.sql("SHOW TABLES in global_temp").show()
spark2.sql("SHOW TABLES in global_temp").show()

# COMMAND ----------

# WORKS, stores is created with in global_temp
spark.sql("SELECT * FROM global_temp.stores").show()

# COMMAND ----------

# WORKS, stores is created with in global_temp
spark2.sql("SELECT * FROM global_temp.stores").show()

# COMMAND ----------

