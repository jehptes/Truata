from operator import add

def explode(row):
    """
    Function to explode rows of RDD
    """
    for k in row:
        yield k

# Explode rows of rdd
full = rdd_load.flatMap(explode) 

# Find frequency count and order results
product_count = full.map(lambda x: (x,1)).reduceByKey(add).sortBy(lambda x: x[1], False)


product_count.coalesce(1,True).saveAsTextFile("abfss://testing@asapframeworkdldev.dfs.core.windows.net/foubs/out_1_3.txt")