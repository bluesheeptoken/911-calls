# 911 Calls avec ElasticSearch

## Import du jeu de données

Pour importer le jeu de données, complétez le script `import.js` (ici aussi, cherchez le `TODO` dans le code :wink:).

Exécutez-le ensuite :

```bash
npm install
node import.js
```

Vérifiez que les données ont été importées correctement grâce au shell (le nombre total de documents doit être `153194`) :

```
GET <nom de votre index>/_count
```

## Requêtes

À vous de jouer ! Écrivez les requêtes ElasticSearch permettant de résoudre les problèmes posés.

```
PUT call
{
  "mappings": {
    "call": {
      "properties": {
        "location": {
          "type": "geo_point"
        }
      }
    }
  }
}

# Cause of call
GET call/call/_search
{
    "size": 0,
    "aggs" : {
        "title" : {
            "terms": {
                "field": "title.keyword"
            }
        }
    }
}

# Top 3 call for Overdose
GET call/call/_search
{
  "query":
  {
    "match":
    {
      "cause" : "OVERDOSE"
    }
  },
  "size": 0,
  "aggs" : {
    "city" : {
      "terms": {
        "field": "twp.keyword",
        "size": 3
      }
    }
  }
}

# Histogram call per month
GET call/call/_search?size=0
{
  "aggs" : 
  {
    "by_month" : 
    {
      "date_histogram": 
      {
        "field": "@timestamp",
        "interval":  "month",
        "format": "YY-MM",
        "order": {"_count": "desc"}
      }
    }
  }
}

# Geolocalisation
GET call/call/_search
{
  "query":
  {
    "bool" :
    {
      "must" : 
      {
        "match_all" :{}
      },
      "filter" :
      {
        "geo_distance" : 
        {
          "distance" : "500m",
          "location" : {"lon" : -75.283783, "lat" : 40.241493}
        }
      }
    }
  }
}
```

## Kibana

Dans Kibana, créez un dashboard qui permet de visualiser :

* Une carte de l'ensemble des appels
* Un histogramme des appels répartis par catégories
* Un Pie chart réparti par bimestre, par catégories et par canton (township)

Pour nous permettre d'évaluer votre travail, ajoutez une capture d'écran du dashboard dans ce répertoire [images](images).

### Timelion
Timelion est un outil de visualisation des timeseries accessible via Kibana à l'aide du bouton : ![](images/timelion.png)

Réalisez le diagramme suivant :
![](images/timelion-chart.png)

Envoyer la réponse sous la forme de la requête Timelion ci-dessous:  

```
TODO : ajouter la requête Timelion ici
```
