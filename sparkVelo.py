from pyspark import SparkContext
import sys
from math import radians, cos, sin, asin, sqrt
import re

# Calcul de distance entre 2 coordonnees GPS : retourne le resultat en km
def distance(lon1, lat1, lon2, lat2):
	lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
	dlon = lon2 - lon1 
	dlat = lat2 - lat1 
	a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
	c = 2 * asin(sqrt(a)) 
	km = 6371 * c
	return km


sc = SparkContext(appName='sparkVelo', master='local[2]')

# Recherche autour du site les Nefs, Nantes
lieu = [47.2063698, -1.5643358]
dist_max = 1.0
capacite_min = 5

lines = sc.textFile(sys.argv[1])
# Lire le header du fichier csv
header = lines.first()
# filter : retirer le header des donnees
# map : split chaque ligne par ","
# map : recuperer le nom, l'adresse, la capacite et le distance
# filter : retenir les lieux ayant 5 places libres et etant a moins de 500m
values = lines.filter(lambda line: line != header)\
.map(lambda line: line.split('","'))\
.map(lambda line: (line[1], line[2], line[10], distance(float((re.sub('[^0-9-.,]','',line[15])).split(',')[0]), float((re.sub('[^0-9-.,]','',line[15])).split(',')[1]), float(lieu[0]), float(lieu[1]))))\
.filter(lambda (nom, adresse, capacite, distance): int(capacite)>=capacite_min and distance<=dist_max)\
#.collect()

values.saveAsTextFile(sys.argv[2])


