import json
import requests
from datetime import datetime

ELASTIC = "http://localhost:9200" #dirección en la que está funcionando elastic

"""
    Artículos de la sección "Internacional" que contengan la frase "crisis económica"
"""

query = {
    "size" : 10000,
    "query" : {
        "bool" : {
            "must" : [
                {"match" : {"seccion" : "Internacional"}},
                {"match_phrase" : {"cuerpo" : "crisis económica"}}
            ]
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/norte/_search",json=query).text)["hits"]["hits"]

for re in res:
    r = re["_source"]
    
    print('"%s" [score %0.2f]' % (r["titulo"],re["_score"]))
    
    if "resumen" in r:
        print("\t"+r["resumen"])
    print("")


"""
    Artículos publicados tal día como hoy en 2006 dentro de la sección Televisión
"""

day = datetime.now().date().day
month = datetime.now().date().month

query = {
    "size" : 10000,
    "query" : {
        "bool" : {
            "must" : [
                {"match" : { "seccion" : "Televisión" }},
                {"match" : { "fecha" : str(day).zfill(2)+"-"+str(month).zfill(2)+"-2006" }}
            ]
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/norte/_search",json=query).text)["hits"]["hits"]

for re in res:
    r = re["_source"]
    
    print('"%s"' % (r["titulo"]))
    
    if "resumen" in r:
        print("\t"+r["resumen"])
    print("")


"""
    Artículos publicados durante el mes de mayo que contengan la palabra "Eurovisión"
"""

query = {
    "size" : 10000,
    "query" : {
        "bool" : {
            "must" : [
                {"match" : { "cuerpo" : "eurovisión" }},
                {"range" : { 
                    "fecha" : {
                        "gte" : "05-2006",
                        "lte" : "06-2006",
                        "format" : "MM-yyyy"
                    }
                }}
            ]
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/norte/_search",json=query).text)["hits"]["hits"]

for re in res:
    r = re["_source"]
    
    print('"%s"' % (r["titulo"]))
    if "resumen" in r:
        print("\t"+r["resumen"])
    print("")