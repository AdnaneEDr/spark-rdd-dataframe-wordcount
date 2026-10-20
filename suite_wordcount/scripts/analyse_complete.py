"""Suite du projet Spark : nouvelles activités TP-0 et WordCount TP-1.
Lancer depuis suite_wordcount avec spark-submit --master local[2] scripts/analyse_complete.py
Le fichier ARRETER_UI permet de fermer l'application après les captures.
"""
from pathlib import Path
from collections import Counter
import re, json, time, os, hashlib, urllib.request
from pyspark.sql import SparkSession, functions as F

BASE=Path(__file__).resolve().parent.parent
RESULT=BASE/'resultats'; JOUR=BASE/'journaux'
RESULT.mkdir(exist_ok=True); JOUR.mkdir(exist_ok=True)
MOT=re.compile(r'[a-zàâäçéèêëîïôöùûüÿœæ]+')
SEP=r'[^a-zàâäçéèêëîïôöùûüÿœæ]+'
VIDES={'les','des','que','qui','dans','pour','pas','une','est'}
M={}
def section(name, text):
    if not isinstance(text,str): text=json.dumps(text,ensure_ascii=False,indent=2)
    (JOUR/(name+'.txt')).write_text(text,encoding='utf-8')
    print('\n### '+name+'\n'+text,flush=True)
def ordered(d,n=15): return sorted(d.items(),key=lambda x:(-x[1],x[0]))[:n]
def words_df(lines):
    return lines.select(F.explode(F.split(F.lower('value'),SEP)).alias('mot')).filter(F.length('mot')>2)
spark=SparkSession.builder.appName('Suite TP Spark - WordCount RDD DataFrame SQL').getOrCreate()
sc=spark.sparkContext; sc.setLogLevel('WARN')
M['environnement']={'spark':spark.version,'master':sc.master,'parallelisme':sc.defaultParallelism,'python':os.sys.version.split()[0],'ui':sc.uiWebUrl,'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions')}
M['somme_carres']=spark.range(1,101).selectExpr('sum(id*id) AS somme_carres').first()[0]
M['carres_rdd']=sc.parallelize([1,2,3,4],2).map(lambda x:x*x).collect()
section('12_complements_tp0',{'environnement':M['environnement'],'somme_carres_1_100':M['somme_carres'],'carres_RDD':M['carres_rdd']})
mini=str(BASE/'mini.txt'); livres=str(BASE/'livres')
lignes=sc.textFile(mini,minPartitions=2).cache()
raw=lignes.collect()
mots=lignes.flatMap(lambda l:MOT.findall(l.lower())); grands=mots.filter(lambda m:len(m)>2)
paires=grands.map(lambda m:(m,1)); compte=paires.reduceByKey(lambda a,b:a+b)
mini_rdd=dict(compte.collect())
M['mini']={'lignes':lignes.count(),'premiere_ligne':lignes.first(),'partitions':lignes.getNumPartitions(),'map_split':lignes.map(lambda l:l.split()).count(),'flatMap_split':lignes.flatMap(lambda l:l.split()).count(),'mots_normalises':mots.count(),'mots_plus_2':grands.count(),'uniques':len(mini_rdd),'compte':ordered(mini_rdd,50),'top3':ordered(mini_rdd,3),'plus_5':sorted(set(mots.filter(lambda m:len(m)>5).collect()))}
M['mini']['debug_rdd']=compte.toDebugString().decode()
section('13_mini_rdd',M['mini'])
df=spark.read.text(mini); dwords=words_df(df)
dfcount=dwords.groupBy('mot').count().orderBy(F.desc('count'),'mot')
dfcount.show(); df.printSchema()
dfmini={r.mot:r['count'] for r in dfcount.collect()}
dwords.createOrReplaceTempView('mots_mini')
sqlmini=spark.sql('SELECT mot, COUNT(*) AS n FROM mots_mini GROUP BY mot ORDER BY n DESC, mot LIMIT 3')
M['mini']['sql_top3']=[list(r) for r in sqlmini.collect()]
M['mini']['egalite_rdd_df']=mini_rdd==dfmini
section('14_mini_dataframe_sql',{'schema':df.schema.simpleString(),'table':dfcount._jdf.showString(20,0,False),'sql_top3':M['mini']['sql_top3'],'egalite_RDD_DataFrame':M['mini']['egalite_rdd_df']})

# Le corpus brut inclut les en-têtes et licences Gutenberg, comme dans les scripts du sujet.
clignes=sc.textFile(livres).cache()
cmots=clignes.flatMap(lambda l:MOT.findall(l.lower())).filter(lambda m:len(m)>2).cache()
ccompte=cmots.map(lambda m:(m,1)).reduceByKey(lambda a,b:a+b).cache()
full_rdd=dict(ccompte.collect())
M['corpus']={'partitions_rdd':clignes.getNumPartitions(),'lignes':clignes.count(),'mots':cmots.count(),'uniques':len(full_rdd),'top15':ordered(full_rdd),'fichiers':sorted(p.name for p in (BASE/'livres').glob('*.txt'))}
section('15_livres_rdd',M['corpus'])
ccompte.sortBy(lambda kv:(-kv[1],kv[0])).saveAsTextFile(str(RESULT/'wordcount_rdd'))
cdf=spark.read.text(livres); cdwords=words_df(cdf).cache()
cdcount=cdwords.groupBy('mot').count().orderBy(F.desc('count'),'mot').cache()
full_df={r.mot:r['count'] for r in cdcount.collect()}
cdwords.createOrReplaceTempView('mots_livres')
sqlrows=spark.sql('SELECT mot, COUNT(*) AS n FROM mots_livres GROUP BY mot ORDER BY n DESC, mot LIMIT 5').collect()
cdcount.write.mode('errorifexists').option('header',True).csv(str(RESULT/'wordcount_df'))
plan=cdcount._jdf.queryExecution().toString()
(JOUR/'19_plan_dataframe.txt').write_text(plan,encoding='utf-8')
M['corpus']['egalite_rdd_df']=full_rdd==full_df
section('16_livres_dataframe_sql',{'top15':ordered(full_df),'sql_top5':[list(r) for r in sqlrows],'egalite_complete_RDD_DataFrame':full_rdd==full_df,'sorties':['resultats/wordcount_rdd','resultats/wordcount_df']})

sans_rdd=dict(cmots.filter(lambda m:m not in VIDES).map(lambda m:(m,1)).reduceByKey(lambda a,b:a+b).collect())
sans_df={r.mot:r['count'] for r in cdwords.filter(~F.col('mot').isin(sorted(VIDES))).groupBy('mot').count().collect()}
M['sans_vides']={'liste':sorted(VIDES),'mots':sum(sans_df.values()),'uniques':len(sans_df),'top15':ordered(sans_df),'egalite_rdd_df':sans_rdd==sans_df}
section('17_sans_mots_vides',M['sans_vides'])

# Métadonnée de fichier demandée par l'exercice, avec repli explicite si non supportée.
try:
    avec_livre=cdf.select(F.col('_metadata.file_name').alias('livre'),F.explode(F.split(F.lower('value'),SEP)).alias('mot'))
    perbook=avec_livre.filter(F.length('mot')>2).groupBy('livre').count().orderBy('livre').collect()
    metadata='_metadata.file_name'
except Exception as e:
    M['metadata_erreur']=str(e)[:1500]
    avec_livre=cdf.select(F.regexp_extract(F.input_file_name(),r'([^/]+)$',1).alias('livre'),F.explode(F.split(F.lower('value'),SEP)).alias('mot'))
    perbook=avec_livre.filter(F.length('mot')>2).groupBy('livre').count().orderBy('livre').collect()
    metadata='input_file_name() : repli car _metadata.file_name non pris en charge'
M['par_livre']={'methode':metadata,'filtre':'mots de plus de 2 lettres, comme WordCount','resultats':[[r.livre,r['count']] for r in perbook]}
section('18_par_livre',M['par_livre'])

lazy={}
try:
    x=sc.textFile(str(BASE/'fichier_qui_n_existe_pas.txt')); lazy['creation']='OK, aucune action exécutée'
except Exception as e: lazy['creation']=type(e).__name__+': '+str(e)[:500]
try: x.count(); lazy['count']='OK inattendu'
except Exception as e: lazy['count']=type(e).__name__+': '+str(e)[:1100]
M['paresseux']=lazy;section('20_evaluation_paresseuse',lazy)

# Référence indépendante pour vérifier les résultats complets et les fichiers exportés.
ref=Counter()
for f in sorted((BASE/'livres').glob('*.txt')):
    with f.open(encoding='utf-8-sig') as stream:
        for line in stream: ref.update(m for m in MOT.findall(line.lower()) if len(m)>2)
import ast,csv
saved_rdd={}
for f in (RESULT/'wordcount_rdd').glob('part-*'):
    for line in f.read_text(encoding='utf-8').splitlines():
        k,v=ast.literal_eval(line);saved_rdd[k]=v
saved_df={}
for f in (RESULT/'wordcount_df').glob('part-*.csv'):
    with f.open(encoding='utf-8',newline='') as stream:
        for row in csv.DictReader(stream):saved_df[row['mot']]=int(row['count'])
M['validation']={'reference_python':dict(ref)==full_rdd==full_df,'export_rdd':saved_rdd==full_rdd,'export_csv':saved_df==full_df,'sans_vides':sans_rdd==sans_df,'somme_par_livre':sum(r['count'] for r in perbook)==sum(ref.values()),'mini_exact':mini_rdd=={'mer':4,'est':2,'calme':1,'belle':1,'capitaine':1,'regarde':1,'nautilus':1,'plonge':1,'sous':1},'somme_carres':M['somme_carres']==338350,'map_flatmap':M['mini']['map_split']==3 and M['mini']['flatMap_split']==20}
assert all(M['validation'].values()), M['validation']
section('21_validation',M['validation'])
(RESULT/'mesures.json').write_text(json.dumps(M,ensure_ascii=False,indent=2),encoding='utf-8')
for name,data in [('top15',M['corpus']['top15']),('top15_sans_vides',M['sans_vides']['top15']),('par_livre',M['par_livre']['resultats'])]:
    with (RESULT/(name+'.csv')).open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.writer(stream);writer.writerow(['livre' if name=='par_livre' else 'mot','nombre']);writer.writerows(data)
section('22_ui_prete',f'Application : {sc.applicationId}\nURL : {sc.uiWebUrl}\nTous les calculs et exports sont terminés.\nCréer ARRETER_UI dans suite_wordcount pour terminer la session.')
print('PRET_CAPTURE_WORDCOUNT',flush=True)
for _ in range(1200):
    if (BASE/'ARRETER_UI').exists(): break
    time.sleep(1)
spark.stop()
print('SESSION_TERMINEE',flush=True)
