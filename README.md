# Apache Spark : RDD, DataFrame et SQL

Projet Spark 4.2.0 avec rapport consolidé, captures et résultats réellement exécutés.

- [Compte rendu consolidé](Compte_rendu_Spark_Mis_a_jour.pdf) : installation, puis compléments et WordCount.
- [Guide de la suite](suite_wordcount/README_SUITE.txt) : données, scripts et reproduction.
- `suite_wordcount/scripts/` : WordCount RDD/DataFrame, SQL et exercices.
- `suite_wordcount/resultats/` : exports complets et vérifications indépendantes.
- `captures/` et `suite_wordcount/captures/` : 21 captures au total.

Les fréquences RDD et DataFrame concordent : 496 765 occurrences, 31 052 mots distincts. Les données Gutenberg brutes incluent leurs en-têtes et licences.

## Reproduction

Windows : `suite_wordcount/scripts/reproduire_suite.ps1`, en adaptant les paramètres JavaHome, SparkHome et PythonExe. Java et Spark ne sont pas embarqués. Le support Hadoop Windows fourni conserve sa provenance et ses empreintes. La procédure écrit dans un nouveau dossier horodaté pour préserver les résultats archivés.

Les chemins locaux des rapports et journaux décrivent la machine d’exécution initiale.
