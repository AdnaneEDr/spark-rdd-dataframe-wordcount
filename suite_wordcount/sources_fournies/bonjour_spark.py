# -*- coding: utf-8 -*-
"""TP-0 : premier programme Spark (test de l'installation).
Lancement :  spark-submit scripts/bonjour_spark.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bonjour Spark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Version de Spark :", spark.version)
print("Master           :", spark.sparkContext.master)
print("Cœurs utilisés   :", spark.sparkContext.defaultParallelism)

nombres = spark.range(1, 1_000_001)          # 1, 2, ..., 1 000 000
nombres.selectExpr("sum(id) AS somme").show()

spark.stop()
