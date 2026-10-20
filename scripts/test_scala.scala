spark.sparkContext.setLogLevel("WARN")
spark.range(1,1001).selectExpr("sum(id) AS somme").show()
