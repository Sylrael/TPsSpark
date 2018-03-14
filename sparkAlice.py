from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import sys

if __name__ == "__main__":
	chars_to_remove = ",.;:?!\"-'*"
	#initialize config
	#sc = SparkContext(master='local[2]', appName='SparkMeteoDataFrame')
	spark = SparkSession\
		.builder\
		.appName("sparkAliceDataFrame")\
		.config("master", "local[2]")\
		.getOrCreate()
		
	sc = spark.sparkContext
	#read file from stdin directory/file
	lines = sc.textFile(sys.argv[1])
	
	# flatMap : retrieve all words
	# map : remove bad chars at the beginning and at the end of the word
	# filter : remove words containing '@' and '/' and empty words
	# map : map for word count (word,1)
	# reduceByKey : count all words frequencies
	# map : word with word, frequency and length
	values = lines.flatMap(lambda line: line.lower().split(' '))\
	.map(lambda mot: mot.strip(chars_to_remove))\
	.filter(lambda mot: '@' not in mot and '/' not in mot and mot!='')\
	.map(lambda mot: (mot, 1))\
	.reduceByKey(lambda count1, count2: count1 + count2)\
	.map(lambda (mot, count): Row(word=mot, frequency=int(count), length=int(len(mot))))\
	
	# Create DataFrame from RDD
	df = spark.createDataFrame(values).cache()
	df.printSchema()
	df.createTempView("Alice")
	
	#Request
	# Longest words
	# Most frequent words with 4 letters
	# Most frequent words with 15 letters
	
	# Request with methods
	df.orderBy(df.length, ascending=False).show()
	df.filter("length=4").orderBy(df.frequency, ascending=False).show()
	df.filter("length=4").orderBy(df.frequency, ascending=False).show()
	
	# Request with SQL queries
	#spark.sql("SELECT * from Alice ORDER BY length DESC").show()
	#spark.sql("SELECT * from Alice WHERE length=4 ORDER BY frequency DESC").show()
	#spark.sql("SELECT * from Alice WHERE length=15 ORDER BY frequency DESC").show()