# Après avoir exécuté session_tp.ps1 dans la même session PowerShell
$env:SPARK_DIST_CLASSPATH = "$env:SPARK_HOME/examples/jars/*"
$env:JAVA_TOOL_OPTIONS = '-Dspark.master=local[2] -Dspark.ui.port=4041'
& "$env:SPARK_HOME/bin/spark-class.cmd" org.apache.spark.examples.SparkPi 10
Remove-Item Env:JAVA_TOOL_OPTIONS
Remove-Item Env:SPARK_DIST_CLASSPATH
