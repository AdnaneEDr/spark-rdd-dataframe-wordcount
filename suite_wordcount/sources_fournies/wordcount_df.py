# -*- coding: utf-8 -*-
"""TP-1 : WordCount avec l'API DataFrame (et SQL).
Lancement (depuis le dossier spark-tp) :
    spark-submit --master "local[*]" scripts/wordcount_df.py
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LIVRES = sys.argv[1] if len(sys.argv) > 1 else "livres"
SORTIE = sys.argv[2] if len(sys.argv) > 2 else "resultats/wordcount_df"

spark = SparkSession.builder.appName("WordCount-DataFrame").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Lire : un tableau à une colonne "value" (une ligne de texte par rangée)
lignes = spark.read.text(LIVRES)
lignes.printSchema()

# 2. Découper chaque ligne en mots, une rangée par mot
mots = lignes.select(
    F.explode(F.split(F.lower("value"), r"[^a-zàâäçéèêëîïôöùûüÿœæ]+")).alias("mot")
)
mots = mots.filter(F.length("mot") > 2)

# 3. Compter et trier : "combien de rangées par mot ?"
compte = mots.groupBy("mot").count().orderBy(F.desc("count"))
compte.show(15)

# 4. La même question en SQL
mots.createOrReplaceTempView("mots")
spark.sql("""
    SELECT mot, COUNT(*) AS n
    FROM mots
    GROUP BY mot
    ORDER BY n DESC
    LIMIT 5
""").show()

# 5. Le plan d'exécution choisi par Spark
compte.explain()

# 6. Sauvegarder en CSV (le dossier est remplacé à chaque lancement)
compte.write.mode("overwrite").option("header", True).csv(SORTIE)
print("Résultat écrit dans :", SORTIE)

spark.stop()
