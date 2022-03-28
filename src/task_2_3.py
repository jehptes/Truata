out_2_3 = (df.filter('price > 5000 and review_scores_value = 10')
          .agg(f.avg('bathrooms').alias('avg_bathrooms'), f.avg('bedrooms').alias('avg_bedrooms'))
           )   

# check results
out_2_3.show()