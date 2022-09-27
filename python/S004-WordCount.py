# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC create a words.txt file in your laptop, have some data like
# MAGIC 
# MAGIC python Spark APACHe
# MAGIC     spark PYTHON kafka
# MAGIC AWS apple Orange apple
# MAGIC        
# MAGIC Kafka Stream   
# MAGIC     
# MAGIC Spark Stream

# COMMAND ----------

# MAGIC %md
# MAGIC uploads words.txt into sfsep22/words.txt into databricks Data section

# COMMAND ----------

fileRdd = sc.textFile('/FileStore/tables/sfsep22/words.txt')

# COMMAND ----------

fileRdd.getNumPartitions()

# COMMAND ----------

fileRdd.glom().collect()

# COMMAND ----------

fileRdd.count()

# COMMAND ----------

fileRdd.first()

# COMMAND ----------

fileRdd.take(2) # doesn't read all data from partitions, instead it pick few records from partition 0...

# COMMAND ----------

# \ line continuation, \NO EXTRA SPACE
lineRdd = fileRdd.map (lambda l: l.strip().lower())\
                 .filter (lambda l: l != '')

lineRdd.collect()

# COMMAND ----------

r = lineRdd.map(lambda l: l.split(' '))
print(r.count()) 
r.collect()

# COMMAND ----------

wordRdd = lineRdd.flatMap(lambda l: l.split(' '))
print(wordRdd.count())
wordRdd.collect()

# COMMAND ----------

# structure to make key, value pair (paird rdd) (word, 1)
pairRdd = wordRdd.map (lambda word: (word, 1)) # (apple, 1) is called tuple of 2
pairRdd.collect()

# COMMAND ----------

# Input
"""
('spark', 1), <--   spark is first time, it doesn't call reducer lambda acc, value: acc + valu func, instead put the data into table/result
('apache', 1), <-- apache is first time, it doesn't call reducer lambda acc, value: acc + valu func, instead put the data into table/result
('spark', 1), <- spark, is second time, now it calls reduce function, it picks acc from table, 1 (value) from data, calls the reducer function lambda acc, value: acc + value, (acc/1, value/1) => acc (1) + value (1) = 2, now result 2 is updated in the table 
('python', 1), <- python, first time, reducer is not called, placed directly inot table
 ('spark', 1), <- spark agian, calls lambda , acc is 2, value is 1, 2 + 1 = 3, the result 3 is updated in the table.. 
('apache', 1), <- apache again, acc is 1, value is 1, acc + value (1 + 1) = 2, result is updated in table
"""

"""
Imagine a table interface for reduceby key

word       acc
spark      3   [was 2]   [was 1]
apache     2   [was 1]
python    1
"""

wordCountRdd = pairRdd.reduceByKey(lambda acc, value: acc + value)
wordCountRdd.collect()

# COMMAND ----------

# ( for indentation or grouping, no need for \ for continue line)

wordCountRdd2 = (
               sc.textFile('/FileStore/tables/sfsep22/words.txt')
                .map (lambda l: l.strip().lower())
                 .filter (lambda l: l != '')
                .flatMap(lambda l: l.split(' '))
              .map (lambda word: (word, 1))
              .repartition(4)
              .reduceByKey(lambda acc, value: acc + value)
)

wordCountRdd2.collect()

# COMMAND ----------

wordCountRdd2.glom().collect()

# COMMAND ----------

wordCountRdd2.glom().collect()

# COMMAND ----------

