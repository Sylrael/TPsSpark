# TpSpark1

## RDD

### Statistiques sur des données météo avec l'API RDDs

`sparkMeteo.py`

Température max par mois sur les deux années 1901 et 1902

### Trouver une place libre pour vélo

`sparkVelo.py'

Récupérer la liste de toutes les stations de vélo disponibles autour d'un lieu

Capacité de 5+
Lieu à 1km autour du site

## DataFrame

### Statistiques sur des données météo avec les DataFrames

`sparkMeteoDataframe.py`

### Statistiques sur un texte avec des DataFrames

`sparkAlice.py`

Utiliser les règles d’extraction des lignes et des mots du texte :
- On considère que les mots sont séparés par des espaces.
- Les caractères `,.;:?!"-'*` en début et en fin de mot seront supprimés.
- Les mots contenant les caractères `@` ou `/` seront supprimés.
- Les mots doivent être ramenés en caractères minuscules

Résultats
- le mot le plus long du texte
- le mot de quatre lettres le plus fréquent
- le mot de quinze lettres le plus fréquent