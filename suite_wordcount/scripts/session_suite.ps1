param(
    [string]$JavaHome = 'C:\Users\lenovo\Documents\Codex\2026-10-20\fa-2\work\java\jdk-17.0.20.1+1',
    [string]$SparkHome = 'C:\Users\lenovo\Documents\Codex\2026-10-20\fa-2\work\spark',
    [string]$PythonExe = 'C:\Users\lenovo\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
)
$suite = Split-Path $PSScriptRoot -Parent
$env:JAVA_HOME = $JavaHome
$env:SPARK_HOME = $SparkHome
$env:HADOOP_HOME = Join-Path $suite 'outils/hadoop'
$env:PATH = "$env:HADOOP_HOME/bin;$JavaHome/bin;$SparkHome/bin;$env:PATH"
$env:PYSPARK_PYTHON = $PythonExe
$env:PYSPARK_DRIVER_PYTHON = $PythonExe
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:PYTHONIOENCODING = 'utf-8'
Write-Output 'Variables de la suite prêtes (pour ce processus uniquement).'
