Une fois Docker et Docker-compose installés, ouvrez un terminal, naviguez vers le dossier
où ce _repo_ est stocké et lancez éxécutez la commande
```shell
docker compose up airflow-init
```
pour initialiser Airflow. 

Pour lancer Airflow, vous pourrez ensuite utiliser la commande
```shell
docker compose up
```
__Note:__ Il est possible que vous deviez exécuter cette commande 2 fois si la première 
retourne une erreur. 

Pour l'arrêter, la commande suivante suffira
```shell
docker compose down
```

Il vous faudra enfin exécuter la commande:
```shell
pip install requirements.txt
```
qui vous permettra de développer le DAG de ce cours.

# Description du pipeline que l'on va développer
Le pipeline que nous développerons consistera à:
1. Récupérer les données de l'API de [The Open Sky Network](https://opensky-network.org/). 
Ces données sont ces de tous les avions en vol à l'heure actuelle (il y a évidemment une limitation)
2. Organiser les données par compagnie aérienne
3. Charger les données dans DuckDB (pour garder tout ceci en local)
4. Vérifier la qualité des données
