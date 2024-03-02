from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello"

words = spark.sparkContext.parallelize(text.split(" "))

wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in wordCounts.collect():
    print(wc[0], wc[1])

spark.stop()

# def count_words(text):
#     spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()
#
#     words = spark.sparkContext.parallelize(text.split(" "))
#
#     wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
#
#     for wc in wordCounts.collect():
#         print(wc[0], wc[1])
#
#     spark.stop()
