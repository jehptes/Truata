"""
This assignment was done using an Azure synapse analytics notebook. 
"""

# vectore assembler
assembler = VectorAssembler(inputCols=['sepal_length', 'sepal_width', 'petal_length', 'petal_width'],
                            outputCol="features") 

assembler_df = assembler.transform(df_ml)


# convert class string to numeric 
"""
This does not work in pyspark Azure syanpse analytics. 
Error : 
Py4JJavaError: An error occurred while calling o848.fit.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 70.0 failed 4 times, most recent failure: Lost task 0.3 in stage 70.0 
(TID 106) (vm-e7028927 executor 2): 
org.apache.spark.SparkException: Unsupported data type ObjectType(interface org.apache.spark.sql.Row)
"""

#string_indexer = StringIndexer(inputCol="class", outputCol="classIndex")
#output = string_indexer.fit(assembler_df).transform(assembler_df)


 

"""
As stringindexer was not working, class column was converted  by creating a new column with class int values. 

Identification of string to int conversion

'Iris-virginica':  0

'Iris-setosa': 1

'Iris-versicolor': 2
"""

output = assembler_df.withColumn('class_int', 
                                when(assembler_df['class']=='Iris-virginica', 0).
                                        otherwise( when(assembler_df['class']=='Iris-setosa',1).
                                        otherwise( when(assembler_df['class']=='Iris-versicolor',2).
                                        otherwise('NULL'))
                                ).cast('int')
                                )

# features and class_int column dataset
df_final = output.select("features", "class_int")

# Split the df_final dataframe into train and test dataset
train, test = df_final.randomSplit([0.7, 0.3], seed = 42)

# define logistic regression
lr = LogisticRegression(featuresCol= "features", labelCol="class_int")


# Fit Logistic Regression classifier
lr_model = lr.fit(train)

# Evaluate model on test data
pred_labels = lr_model.evaluate(test)

# Fit Logistic Regression classifier on full dataset
final_model = lr.fit(df_final)


# define Truata exercise test data
pred_data = spark.createDataFrame(
 [(5.1, 3.5, 1.4, 0.2),
 (6.2, 3.4, 5.4, 2.3)],
 ["sepal_length", "sepal_width", "petal_length", "petal_width"])

# build assembler for truata test dataframe
iris_type = assembler.transform(pred_data)

# Run the final model on test data set 
res = final_model.transform(iris_type)

predictions = res.select('prediction')

predictions.show()