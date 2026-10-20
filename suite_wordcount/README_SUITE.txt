SUITE DU PROJET SPARK - 20 octobre 2026

Le JDK, Spark, Python et la configuration existants sont réutilisés.
Les installations, SparkPi et calculs similaires ne sont pas refaits.
Cette suite ajoute : support Hadoop Windows, parallélisme, somme des carrés,
WordCount mini/cinq livres en RDD, DataFrame et SQL, exercices et exports.

Le rapport consolidé est à la racine du ZIP (partie initiale + suite sans doublon).
Les 9 nouvelles captures sont dans captures/. Les captures de journaux sont
des vues dans un navigateur de résultats réels, pas des fenêtres PowerShell.
Les figures Spark UI sont des captures directes de l'application en exécution.

Résultats : 496765 occurrences (mots > 2 lettres), 31052 mots distincts.
Les livres bruts incluent les en-têtes et licences Gutenberg.
Référence Python indépendante, égalité RDD/DataFrame et relecture des exports : OK.

Reproduction : scripts/reproduire_suite.ps1, paramètres JavaHome, SparkHome,
PythonExe à adapter sur un autre poste. Les résultats archivés sont préservés :
une nouvelle exécution écrit dans executions/<horodatage>.
Créer ARRETER_UI dans cette nouvelle exécution pour fermer l'interface.
Les scripts WordCount autonomes refusent un dossier de sortie existant.
Le contrôle du fichier absent produit volontairement une exception capturée.
Sources fournies conservées dans sources_fournies/ ; ne pas les lancer sur
les résultats archivés car elles comportent un effacement ou un écrasement.
Binaires Java et Spark non inclus ; outils Hadoop Windows inclus avec provenance.
