# Databricks notebook source
# Spark context is driver component, part of every driver
# Only 1 spark context per driver
# in Databricks notebook, spark context already created and initialized
# Spark context is known as sc variable name

sc # SHIFT + Enter

# Master - Local [8], local means embedded mode , Driver and Executor runs on Single JVM
#                     8 here means, paralellel tasks can run at any time
#                     This VM has 2 vCores, Databricks, assigned, 4 tasks per vCore.. = 8 tasks

# COMMAND ----------

# Create RDD using spark context
# intellisense in sparl shell,.. sc.<TAB><TAB><TAB> you can see the avaialble functions or properties
# lazy creation of RDD, means, no memory or no partitions no task or no job no stage created at this moment
# partition, data loading all take place when apply ACTION
# no executor used at this moment

# THIS APPLICATION/Shell is DRIVER program
# RDD references is with driver
rdd = sc.parallelize ( range (1, 10) )

# COMMAND ----------

# TRANSFORMATION , logic which applied on the data stored in the RDD [partition]
# TRANSFORMATION is lazy, no exeuction, no memory/no partition/no exeuctors involved here
# TRANSFOR<ATION create another RDD as result, known is child rdd or rdd lineage
# rdd is parent, oddRdd is a child 
# n is a number from parant rdd, 1,2,3...9 (range(1, 10))
# fitler is TRANSFORMATION, allow n values when n is odd number, even numbers are filtered
# filter code which is lambda n: n % 2 == 1 is TASK, TASK is applied on PARTITION
oddRdd = rdd.filter (lambda n: n % 2 == 1 ) 

# COMMAND ----------

# TRANSFORMATION, map - 
# n is 1, 3, 5, 7, 9
# outputs are 10, 30, 40, 70, 90
# map and code lambda n: n * 10 is TASK, TASK shall be executed at executor, lazy,
# TASK is not created until we apply ACTIONs on RDD
oddBy10Rdd = oddRdd.map (lambda n: n * 10)

# COMMAND ----------

# ACTION
# NOT LAZY
# ACTION - every action create a JOB
# JOB converts the RDD into DAGs [Directed Acylic Graph]
# DAGs are scheduled to execute one after another
# DAGs are converted into STAGEs
# Stages are executed one after another..
# Each STAGE shall have TASKs
# Each TASK is submitted into Executor, partitions created etc
# Executee execute the TASK with assigned partition
# Result may or may not collected by Driver that depends on ACTION API we call

# collect, ACTION, execute job, collect result back to driver
# don't use collect in projects [use only for debug purpose, comment out or remove collect code]

result = oddBy10Rdd.collect()
print(result)

# COMMAND ----------

# we call action twice here, it create 2 differnt JOB, execute them indepdently..
result1 = oddBy10Rdd.collect()
print(result1)

result2 = oddBy10Rdd.collect()
print(result2)

# COMMAND ----------

