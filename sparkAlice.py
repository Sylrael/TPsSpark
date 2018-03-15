from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import sys

def filter_by_stopwords(partition):
	from nltk.corpus import stopwords
	stop_words=set(stopwords.words('english'))
	for word in partition:
		if word not in stop_words:
			yield word

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
	# mapPartitions : word not in stopwords
	values = lines.flatMap(lambda line: line.lower().split(' '))\
					.map(lambda mot: mot.strip(chars_to_remove))\
					.filter(lambda mot: '@' not in mot and '/' not in mot and mot!='')\
					.mapPartitions(filter_by_stopwords).cache()

	nbMots = float(values.count())
	
	values = values.map(lambda mot: (mot, 1))\
	.reduceByKey(lambda count1, count2: count1 + count2)\
	.map(lambda (mot, count): Row(word=mot, frequency=float(count)/nbMots, length=int(len(mot))))
	
	# Create DataFrame from RDD
	df = spark.createDataFrame(values).cache()
	#df.printSchema()
	
	#Request
	# Longest words
	# Most frequent words with 4 letters
	# Most frequent words with 15 letters
	
	# Request with methods
	df.orderBy(df.length, ascending=False).show()
	df.filter("length=4").orderBy(df.frequency, ascending=False).show()
	df.filter("length=15").orderBy(df.frequency, ascending=False).show()
	
	#print(values.toDebugString())
	#raw_input("Press Ctrl+c to quit")
	
	# Request with SQL queries
	#df.createTempView("Alice")
	#spark.sql("SELECT * from Alice ORDER BY length DESC").show()
	#spark.sql("SELECT * from Alice WHERE length=4 ORDER BY frequency DESC").show()
	#spark.sql("SELECT * from Alice WHERE length=15 ORDER BY frequency DESC").show()