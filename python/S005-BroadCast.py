# Databricks notebook source
# python object, located in Driver/notebook program memory
# later, this goes into broadcast
sector_dict = {
        "MSFT": "TECH",
        "TSLA": "AUTO",
        "EMR": "INDUSTRIAL"
}

# this goes into RDD partition
stocks = [
    ("EMR", 52.0),
    ("TSLA", 300.0),
    ("MSFT", 100.0)
]

# COMMAND ----------

stocksRdd = sc.parallelize(stocks)

# COMMAND ----------

# WITHOUT BROADCAST, not OPTIMAL
# code below shall be executed in executor, where as we refer sector_dict in code
# how come sector_dict located into driver shall be available in executor?
def enrichStockWithSectorWithoutBroadCast(stock):
    return stock + (sector_dict[stock[0]] ,)

# code marshalling - Python copy the lamnda code to executor system/processor
# now enrichStockWithSectorWithoutBroadCast also shipped to executor on every task
# sector_dict is copied into executor along with every task
# if we have 100 partitions, then we will have 100 tasks, then sector_dict is copied 100 times into exeuctor
enrichedRdd = stocksRdd.map (lambda stock: enrichStockWithSectorWithoutBroadCast(stock))
enrichedRdd.take(5)

# COMMAND ----------

# create broadcasted variabel using Spark Context
# this will ensure that sector_dict is kept in every executor 
# where task shall be running
# lazy evaluation, data shall be copied to executors when we run the job
broadCastSectorDict = sc.broadcast(sector_dict)

# Pyspark see this code, this has reference to broadCastSectorDict
# which is broardcast data, pyspark place the broadCastSectorDict in every executor only 
# 1 time instead of every job
# without broadCast, sector_dict shall be copied to executor for every task
# add sector code with stock at executor level when task running
def enrichStockWithSector(stock):
    return stock + (broadCastSectorDict.value[stock[0]] ,)

# code marshalling - Python copy the lamnda code to executor system/processor
# now enrichStockWithSector also shipped to executor on every task
enrichedRdd = stocksRdd.map (lambda stock: enrichStockWithSector(stock))

enrichedRdd.take(5)

# COMMAND ----------

