# FootballApp
Cette application permet de visualiser des statistiques sur les matchs de foot de l'équipe de France de mars 1980 à aujourd'hui.

### Installation
Placez vous à la racine du projet et lancez `python setup.py bdist_egg`.

### Dépendances
Vous devez avoir installé `Python 2.7.*`, `java 1.8.*`, `pyspark 2.4.*`.

### Lancement
Placez vous à la racine du projet et lancez `spark-submit --master local --py-files dist/FootballApp-0.1-py2.7.egg launch.py filename.csv`

### Tests
Lancez les tests avec `python test/TestSuite.py`.

------------
### Notes sur le code
Les trois parties du sujet sont réparties en trois fichiers ([CleanData](https://github.com/MarionLeHerisson/Spark/blob/master/src/CleanData.py), [Statistics](https://github.com/MarionLeHerisson/Spark/blob/master/src/Statistics.py) et [JoinData](https://github.com/MarionLeHerisson/Spark/blob/master/src/JoinData.py)), et chaque instruction est placée dans une fonction, pour plus de clarté.

La partie statistiques a initialement été faite avec des Window Functions dans un soucis de performance, mais je n'ai pas réussi à correctement grouper les donnée, je suis donc passée à une aggrégation classique, comme on peut le voir dans [ce commit](https://github.com/MarionLeHerisson/Spark/commit/52c0e5b463fefd307804fd755562cb79f3a1e126#diff-c5a3670d080c8859fd5ff02288458438L24-L40).