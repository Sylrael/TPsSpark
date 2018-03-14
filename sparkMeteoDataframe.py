from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import sys

if __name__ == "__main__":

	#initialize config
	#sc = SparkContext(master='local[2]', appName='SparkMeteoDataFrame')
	spark = SparkSession\
		.builder\
		.appName("SparkMeteoDataFrame")\
		.config("master", "local[1]")\
		.getOrCreate()
		
	sc = spark.sparkContext
	#read file from stdin directory/file
	lines = sc.textFile(sys.argv[1])
	
	values = lines.map(lambda ligne: Row(month=ligne[19:21], temperature=ligne[87:92], quality=ligne[92]))
	
	#df = spark.createDataFrame(values, ['month','temperature','quality'])
	df = spark.createDataFrame(values)
	
	df.createTempView("meteo")
	df2 = spark.sql("SELECT CAST(meteo.month AS INT), CAST(meteo.temperature AS INT) FROM meteo WHERE meteo.temperature <> 9999 AND CAST(meteo.quality AS INT) IN (0,1,4,5,9)")
	
	df2.createTempView("meteoFiltered")
	spark.sql("SELECT meteoFiltered.month, min(meteoFiltered.temperature), max(meteoFiltered.temperature), AVG(meteoFiltered.temperature) FROM meteoFiltered GROUP BY meteoFiltered.month ORDER BY meteoFiltered.month ASC").show()
