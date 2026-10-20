param([string]$JavaHome = 'C:\Users\lenovo\Documents\Codex\2026-10-20\fa-2\work\java\jdk-17.0.20.1+1', [string]$SparkHome = 'C:\Users\lenovo\Documents\Codex\2026-10-20\fa-2\work\spark')
$env:JAVA_HOME = $JavaHome
$env:SPARK_HOME = $SparkHome
$env:PATH = "$env:JAVA_HOME\bin;$env:SPARK_HOME\bin;$env:PATH"
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:PYSPARK_PYTHON = 'C:\Users\lenovo\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
java -version
Write-Output "SPARK_HOME = $env:SPARK_HOME"
Write-Output 'Session prête. Exemples :'
Write-Output 'spark-shell --master local[2]'
Write-Output 'run-example.cmd SparkPi 10'
Write-Output 'pyspark --master local[2]'
