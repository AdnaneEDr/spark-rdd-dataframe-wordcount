# -*- coding: utf-8 -*-
"""TP-1 : WordCount avec l'API RDD.
Lancement (depuis le dossier spark-tp) :
    spark-submit --master "local[*]" scripts/wordcount_rdd.py
"""
import re
import shutil
import sys

from pyspark.sql import SparkSession

LIVRES = sys.argv[1] if len(sys.argv) > 1 else "livres"
SORTIE = sys.argv[2] if len(sys.argv) > 2 else "resultats/wordcount_rdd"

# Un mot = une suite de lettres (accents français compris)
MOT = re.compile(r"[a-zàâäçéèêëîïôöùûüÿœæ]+")

# 1. Démarrer Spark : le "driver" (ce programme) prend contact avec Spark
spark = SparkSession.builder.appName("WordCount-RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# 2. Lire : un élément du RDD = une ligne de texte
lignes = sc.textFile(LIVRES)
print("Nombre de partitions :", lignes.getNumPartitions())

# 3. Transformer (rien n'est calculé ici : Spark note la recette)
mots = lignes.flatMap(lambda l: MOT.findall(l.lower()))  # 1 ligne -> n mots
mots = mots.filter(lambda m: len(m) > 2)                 # on ignore le, la, de...
paires = mots.map(lambda m: (m, 1))                      # ("mer", 1)
compte = paires.reduceByKey(lambda a, b: a + b)          # ("mer", 1234)

# 4. Agir : c'est ici que Spark calcule vraiment
print("Nombre total de mots  :", mots.count())
print("Nombre de mots uniques :", compte.count())
print("Les 15 mots les plus fréquents :")
for mot, n in compte.takeOrdered(15, key=lambda kv: -kv[1]):
    print(f"  {mot:<15}{n:>8}")

# 5. Sauvegarder (Spark écrit un DOSSIER ; on efface l'ancien résultat)
shutil.rmtree(SORTIE, ignore_errors=True)
compte.sortBy(lambda kv: -kv[1]).saveAsTextFile(SORTIE)
print("Résultat écrit dans :", SORTIE)

spark.stop()
