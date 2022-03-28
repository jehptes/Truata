"""
Part 1: Spark RDD API

This assignment was done using an Azure synapse analytics notebook. 

Steps: 
- Downloaded the input to my computer
- Uploaded the file an Azure storage account container.
- Accessed the file on Azure synapse analytics notebook.
"""

import pyspark.sql.functions as f
from pyspark import SparkContext


rdd_load = spark.sparkContext.textFile("abfss://testing@asapframeworkdldev.dfs.core.windows.net/groceries.csv").map(lambda line: line.split(","))