# Réexécution facultative : aucune installation ni ancien test n'est refait.
param([string]$JavaHome = 'C:\Users\lenovo\Documents\Codex\2026-10-20\fa-2\work\java\jdk-17.0.20.1+1', [string]$SparkHome = 'C:\Users\lenovo\Documents\Codex\2026-10-20\fa-2\work\spark', [string]$PythonExe = 'C:\Users\lenovo\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe')
. "$PSScriptRoot/session_suite.ps1" -JavaHome $JavaHome -SparkHome $SparkHome -PythonExe $PythonExe
$suite = Split-Path $PSScriptRoot -Parent
$run = Join-Path $suite ('executions/' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Force $run, "$run/scripts", "$run/resultats", "$run/journaux" | Out-Null
Copy-Item -LiteralPath "$suite/mini.txt" -Destination $run
Copy-Item -LiteralPath "$suite/livres" -Destination $run -Recurse
Copy-Item -LiteralPath "$PSScriptRoot/analyse_complete.py" -Destination "$run/scripts"
Write-Output "Nouvelle exécution : $run"
Write-Output "Créer le fichier $run/ARRETER_UI pour fermer Spark après consultation."
& "$env:SPARK_HOME/bin/spark-submit.cmd" --master 'local[2]' --conf 'spark.driver.host=127.0.0.1' --conf 'spark.driver.bindAddress=127.0.0.1' "$run/scripts/analyse_complete.py"
