out_2_2 = df.agg(f.max('price').cast('string').alias('max_price'), 
                 f.min('price').cast('string').alias('min_price'),
                 f.count(f.lit(1)).cast('string').alias("row_count"))

# check results
out_2_2.show()
