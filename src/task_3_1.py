"""
Part 3: Applied Machine Learning

This assignment was done using an Azure synapse analytics notebook. 

Steps: 
- Downloaded the input to my computer
- Uploaded the file an Azure storage account container.
- Accessed the file on Azure synapse analytics notebook.
"""


from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from pyspark.sql.functions import when, col
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Define file path
abspath = 'abfss://testing@asapframeworkdldev.dfs.core.windows.net/iris.data'

# define schema
schema = StructType([
    StructField("sepal_length", DoubleType(), True),
    StructField("sepal_width", DoubleType(), True),
    StructField("petal_length", DoubleType(), True),
    StructField("petal_width", DoubleType(), True),
    StructField("class", StringType(), True)
    ]
    )

# access file into spark dataframe
df_ml = spark.read.load(abspath, format='csv', schema=schema)

