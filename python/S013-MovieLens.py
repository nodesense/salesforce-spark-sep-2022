# Databricks notebook source
# /FileStore/tables/sfsep22/ratings/ratings.csv
# /FileStore/tables/sfsep22/movies/movies.csv

# how to create schema programatically instead of using inferSchema
from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType
# True is nullable, False is non nullable
movieSchema = StructType()\
                .add("movieId", IntegerType(), True)\
                .add("title", StringType(), True)\
                .add("genres", StringType(), True)

ratingSchema = StructType()\
                .add("userId", IntegerType(), True)\
                .add("movieId", IntegerType(), True)\
                .add("rating", DoubleType(), True)\
                .add("timestamp", LongType(), True)

# COMMAND ----------

# read movie data
# read using dataframe with defind schema
# we can use folder path - all csv in the folder read
# use file path, only that file read

# spark is session, entry point for data frame/sql
# create dataframe from csv file format..reader/loader
# header - True, first line is Header, ignore first line, delimitter 
movieDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(movieSchema)\
                .load("/FileStore/tables/sfsep22/movies/")

movieDf.printSchema()
movieDf.show(2)

# COMMAND ----------

ratingDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(ratingSchema)\
                .load("/FileStore/tables/sfsep22/ratings")

ratingDf.printSchema()
ratingDf.show(2)

# COMMAND ----------

print (movieDf.count())
print(ratingDf.count())

# COMMAND ----------

ratingDf.take(2)

# COMMAND ----------

# show the distinct ratings
ratingDf.select("rating").distinct().show()

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find the movies by total ratings by userId
df = ratingDf\
     .groupBy("movieId")\
     .agg(count("userId").alias("total_ratings"))\
     .sort(desc("total_ratings"))

df.printSchema()
df.show(20)

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  average rating by users sorted by desc
df = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"))\
     .sort(desc("avg_rating"))

print (df.rdd.getNumPartitions())
df.printSchema()
df.show(20)

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  the most popular movies, where as rated by many users, at least movies should be rated by 100 users
# and the average rating should be at least 3.5 and above
# and sort the movies by total_ratings
mostPopularMoviesDf = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"), count("userId").alias("total_ratings") )\
     .filter( (col("total_ratings") >= 100) & (col("avg_rating") >=3.5) )\
     .sort(desc("total_ratings"), desc("avg_rating"))

mostPopularMoviesDf.cache() # MEMORY

mostPopularMoviesDf.printSchema()
mostPopularMoviesDf.show(20)

# COMMAND ----------

# join, inner join by default
# get the movie title for the mostPopularMoviesDf
# join mostPopularMoviesDf with movieDf based on condition that mostPopularMoviesDf.movieId == movieDf.movieId

popularMoviesDf = mostPopularMoviesDf.join(movieDf, mostPopularMoviesDf.movieId == movieDf.movieId)\
                                     .select(movieDf.movieId, "title", "avg_rating", "total_ratings")\
                                     .sort(desc("total_ratings"))

popularMoviesDf.cache()

popularMoviesDf.show(100)


# COMMAND ----------


popularMoviesDf.explain()

# COMMAND ----------

popularMoviesDf.rdd.getNumPartitions()
#popularMoviesDf.rdd.glom().collect()

# COMMAND ----------

# write popularMoviesDf to hadoop with header [by default headers shall not be written]
# overwrite existing files
# 70 plus partitions having approx total of 100 plus records
# write 70 plus files into hadoop
# write is an ACTION function, create job, execute job...
# write doesn't collect data back to driver
# write execute the file write operations from executors to Distributed File systems [HDFS, Directory, S3, ADLS]
# each partition shall have 1 task
# 72 partitions, 72 tasks, 72 parallel tasks shall be executed, they will write the individual partition data into file system, technically you can find approxmately 72 files, in case the partition is empty, no file is generated, but task shall be executed
# reuse popularMoviesDf, it can take from cache
popularMoviesDf.write.mode("overwrite")\
                .option("header", True)\
                .csv("/FileStore/tables/sfsep22/most-popular-movies-many-files")

# COMMAND ----------

# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
# reuse popularMoviesDf, it can take from cache
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .option("header", True)\
                .csv("/FileStore/tables/sfsep22/most-popular-movies")

# COMMAND ----------

# inferSchema will scan csvs and define data types for  youy schema
# inferSchema is executed at driver level, may cause expensive lookup

popularMovies = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/FileStore/tables/sfsep22/most-popular-movies-many-files")

popularMovies.printSchema()
print("Partitions", popularMovies.rdd.getNumPartitions())
popularMovies.show()

# COMMAND ----------

# print true since mostPopularMoviesDf is not yet unpersisted
mostPopularMoviesDf.is_cached

# COMMAND ----------

# clear the cache if we no longer reuse the rdd/df, but we continue spark application for further analytics
popularMoviesDf.unpersist()
popularMoviesDf.is_cached # TRUE if df/rdd is cached, FALSE if df/rdd is not cached

# COMMAND ----------

# MAGIC %md
# MAGIC # CACHE 
# MAGIC 
# MAGIC 1. When to cache?
# MAGIC     when we reuse RDD, DataFrame over multiple ACTIONs, the same repetitive operations [load csv, create partitions, analsys data, join , group by etc.....] shall be done for every ACTION again and again..
# MAGIC     
# MAGIC     I/O Intensive - Input/Output operations are heavy, like reading TB of data
# MAGIC     CPU Intensive - Performing same work again and again.. on same data.. 
# MAGIC     
# MAGIC     
# MAGIC What it mean reusable...
# MAGIC 
# MAGIC df = .......anallytics query....
# MAGIC 
# MAGIC # write to DB using JDBC [one action, read all inputs, process them and write data to DB]
# MAGIC # write to Data lake in some format [one action, read all inputs, process them and write data to HDFS]
# MAGIC # write to KAFKA in some format [one action, read all inputs, process them and write data to KAFKA]
# MAGIC 
# MAGIC ---------
# MAGIC Now cache it..
# MAGIC df = .......anallytics query....
# MAGIC df.cache() now processed analytical results stored into cache [lazy] when the first action is applied
# MAGIC 
# MAGIC 
# MAGIC # write to DB using JDBC [one action, read all inputs, process them and CACHE IT and write data to DB]
# MAGIC # write to Data lake in some format  [Read from CACHE, write data to HDFS]
# MAGIC # write to KAFKA in some format  [Read from CACHE, write data to KAFKA]