"""
Part 2: Spark Dataframe API 

This assignment was done using an Azure synapse analytics notebook. 

Steps: 
- Downloaded the input to my computer
- Uploaded the file an Azure storage account container.
- Accessed the file on Azure synapse analytics notebook.
"""

import pyspark.sql.functions as f
from pyspark import SparkContext 


# Define file path
abspath = 'abfss://testing@asapframeworkdldev.dfs.core.windows.net/part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet'

# Load file into dataframe
df = spark.read.load(abspath, format='parquet')