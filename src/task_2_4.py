out_2_4 = (df.select('accommodates')
             .filter(
                    (df.price == (df.agg(f.min('price').alias('min_price')).collect()[0][0]))  # find lowest price
                    &  
                    (df.review_scores_rating == (df.agg({"review_scores_rating": "max"}).collect()[0][0]))) # for all properties with lowest price, find the one with highest rating
           )

out_2_4.show()
