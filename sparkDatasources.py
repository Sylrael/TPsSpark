# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, lit


if __name__ == "__main__":

	#initialize config
	spark = SparkSession\
		.builder\
		.appName("sparkDatasourcesHive")\
		.config("master", "local[2]")\
		.config("spark.sql.warehouse.dir", '/tmp/sparkwarehouse')\
		.config("spark.sql.parquet.compression.codec", "gzip")\
		.enableHiveSupport()\
		.getOrCreate()
	
	#df = spark.read.json("sys.argv[1]")
	
	#records = df.select(explode(df.records.fields).alias("fields"))

	#champ = records.select(records.fields.city, records.fields.date_start, records.fields.date_end, records.fields.pricing_info, records.fields.address, records.fields.department, records.fields.title)
	
	#champ.write.mode("overwrite").partitionBy("fields.department", "fields.city").parquet("/tmp/sparkwarehouse/events1/key=1")
	
	#marseille = spark.read.parquet("C:/tmp/sparkwarehouse/events/fields.department=Bouches-du-Rhône/fields.city=Marseille")
	#marseilleSoldout = marseille.withColumn("fields.soldout", lit(False))
	#marseilleSoldout.printSchema()
	#marseilleSoldout.write.mode("overwrite").parquet("/tmp/sparkwarehouse/events1/key=2/fields.department=Bouches-du-Rhône/fields.city=Marseille")
	
	#fullTable = spark.read.parquet("/tmp/sparkwarehouse/events")
	#fullTable.printSchema()
	
	mergedTable = spark.read.option("mergeSchema", "true").parquet("/tmp/sparkwarehouse/events1")
	#mergedDF = spark.read.option("mergeSchema", "true").parquet("/tmp/data/test_table")
	mergedTable.printSchema()
	
	
	
	
