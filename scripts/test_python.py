from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("TP1 Spark").getOrCreate()
spark.range(5).show()
spark.range(1, 1001).selectExpr("sum(id) AS somme").show()
print("Partitions shuffle :", spark.conf.get("spark.sql.shuffle.partitions"))
input("Interface Web disponible sur http://localhost:4040. Entrée pour arrêter...")
spark.stop()
