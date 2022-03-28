def explode(row):
    for k in row:
        yield k


unique_elements = rdd_load.flatMap(explode).distinct().collect()

print(*unique_elements, sep = "\n")