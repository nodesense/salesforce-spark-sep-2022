# Databricks notebook source
# sc, spark context 
# PARTITION
# Partition - sub set of whole data
# whole data [ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]
# 6 Partitions [spark make best attempt equally split the partition data, but not allways]
# the whole data set is split into small small partitions
# partition 0: [1, 2]
# partition 1: [3, 4]
# partition 2: [] empty
# partition 3: [5,6]
# partition 4: [ 7, 8, 9 ]
# partition 5: []

# partitions are created when we apply ACTION [not while creating RDD, not while applying transformation on RDD]
# partitions are created inside the executor memory

# when there is not enough memory, partition data can spill over to disk
# partitions are temporary, deleted as soon as the stage is over

# COMMAND ----------

# TASK
# Every partition shall have a TASK assigned

# TASK filter (lambda n : n % 2 == 1)
# 6 partitions = 6 TASKS created, scheduled
# now task is applied on each partition at executor
# now spark ensure that partitions are loaded into executor(s) memory
# TASK is applied on each partition
# At anywhere, A SINGLE PARTITION NEVER GIVEN TO 2 TASKS 
#  1 PARTITION = 1 TASK
# 6 partitions = 6 Tasks

# partition 0: [1, 2] , apply filter task (lambda n : n % 2 == 1)
# partition 1: [3, 4], apply filter task (lambda n : n % 2 == 1)
# partition 2: [] empty, apply filter task (lambda n : n % 2 == 1)
# partition 3: [5,6], apply filter task (lambda n : n % 2 == 1)
# partition 4: [ 7, 8, 9 ], apply filter task (lambda n : n % 2 == 1)
# partition 5: [], apply filter task (lambda n : n % 2 == 1)

# COMMAND ----------

# defualt partition are from configurations based on API we use

print ("default parallism " ,  sc.defaultParallelism)  # 8, local[8] 8 paralleism
print ("default min " , sc.defaultMinPartitions) # used by while reading data from files

rdd = sc.parallelize(range (1, 12) ) # parallelize function takes default partition from sc.defaultParallelism
# get num partitions

print (rdd.getNumPartitions()) # LAZY
# how to collect data stored in each partitions 
# glom( ).collect data from each partition, it doesnt merge result, get list of list/array of array
# but when we do rdd.collect(), it collects data from each partition and merge data at driver level, list as output
data = rdd.glom().collect()  # ACTION
print (data)

# COMMAND ----------

# specifing partition in the code
rdd = sc.parallelize(range (1, 12), 2) # 2 is partition number by developer
print (rdd.getNumPartitions())
data = rdd.glom().collect()  # ACTION, collect data from partitions, maintain list of list [no merging]
print ("glom", data)

print ("collect ", rdd.collect()) # collect data from partitions and merge result into single list

# COMMAND ----------

# increase partition of existing data set
# repartition function to increase partitions,
# the same be useful to reduce partition, however we have coalsece to reduce partition

rdd = sc.parallelize(range (1, 100), 2)
print ("partition ", rdd.getNumPartitions())
print ("glom ", rdd.glom().collect())
# increase partitions
rdd2 = rdd.repartition(10)
print ("partition ", rdd2.getNumPartitions())
print ("glom ", rdd2.glom().collect())

# COMMAND ----------

# reducing partitions
rdd = sc.parallelize(range (1, 100), 10)
print ("partition ", rdd.getNumPartitions())
print ("glom ", rdd.glom().collect())

rdd2 = rdd.coalesce(2)
print ("partition ", rdd2.getNumPartitions())
print ("glom ", rdd2.glom().collect())

# COMMAND ----------

