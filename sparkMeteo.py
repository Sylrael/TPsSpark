from pyspark import SparkContext
import sys

if __name__ == "__main__":

	#initialize config
	sc = SparkContext(master='local[2]', appName='SparkMeteo')

	#read file from stdin directory/file
	lines = sc.textFile(sys.argv[1])
	# map : get date, temperature, quality
	# filter : bad temperature and bad quality
	# map : get month, temperature as Celsius
	# reduce : reduce by month
	# sort : sort by mont
	values = lines.map(lambda ligne: (ligne[15:24], ligne[87:92], ligne[92]))\
	.filter(lambda (date, temp, quality): int(temp)!=9999 and int(quality) in (0,1,4,5,9))\
	.map(lambda (date, temp, quality):((str(date))[4:6], float(temp)/10))\
	.reduceByKey(max)\
	.sortByKey()
	# write max temp per month in stdout file
	values.saveAsTextFile(sys.argv[2])
	