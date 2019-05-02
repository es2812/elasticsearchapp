"""
    Script de Python 3 que realiza consultas a la API REST de Elasticsearch (localhost:9200)
    sobre el índices ya creado: `wine`.

    Dependencias: requests, json
"""

import json
import requests

ELASTIC = "http://localhost:9200" #dirección en la que está funcionando elastic

"""
    Mejores 5 vinos de cada color
"""

print("Mejores 5 vinos de cada color:")
print("")

query = {"query":
            {
                "term" : { "Color" : "red" }
            },
        "sort" : {
            "Score" : "desc"
        },
        "size": 5
}

reds = json.loads(requests.get(ELASTIC+"/wine/_search",json=query).text)["hits"]["hits"]

print("Vinos tintos:")
print("----------------")
for i, w in enumerate(reds):
    wine = w["_source"]
    print("#%d: %s, %d/100" % (i+1, wine["Name"], wine["Score"]))
print("")

query = {"query":
            {
                "term" : { "Color" : "white" }
            },
        "sort" : {
            "Score" : "desc"
        },
        "size": 5
}

whites = json.loads(requests.get(ELASTIC+"/wine/_search",json=query).text)["hits"]["hits"]

print("Vinos blancos:")
print("----------------")
for i, w in enumerate(whites):
    wine = w["_source"]
    print("#%d: %s, %d/100" % (i+1, wine["Name"], wine["Score"]))
print("")

"""
    Precio medio y puntuación máxima del vino por tipo de uva
"""

print("Precio medio y puntuación máxima por tipo de uva:")
print("")

query = { "aggs" : {
                "uvas" : {
                    "terms" : {
                        "field" : "Grape.keyword"
                    },
                    "aggs":{
                        "precio-medio-por-uva": {
                            "avg" : {
                                "field" : "Price"
                            }
                        },
                        "puntuacion-maxima-por-uva" : {
                            "max" : {
                                "field" : "Score"
                            }
                        }
                    }
                } 
    }
}

aggs = json.loads(requests.get(ELASTIC+"/wine/_search",json=query).text)["aggregations"]

uvas = aggs["uvas"]["buckets"]

for obj in uvas:
    print("%s: %d vinos" % (obj["key"], obj["doc_count"]))
    print("Precio medio: $%0.2f" % (obj["precio-medio-por-uva"]["value"]))
    print("Puntuación máxima: %d/100" % (obj["puntuacion-maxima-por-uva"]["value"]))
    print("")

"""
    Número de vinos y precio medio de vinos con año anterior a 2007
"""

print("Número de vinos y precio medio en vinos de años anteriores a 2007:")
print("")

query = {"query":
            {
                "range" : { "Year" : {
                        "lt" : 2007
                    }
                }
            },
            "aggs":{
                "años":{
                    "terms" : {
                        "field" : "Year"
                    },
                    "aggs":{
                        "precio-medio-por-año": {
                            "avg" : {
                                "field" : "Price"
                            }
                        },
                    }
                }
            }
}

años = json.loads(requests.get(ELASTIC+"/wine/_search",json=query).text)["aggregations"]["años"]["buckets"]

for a in años:
    print("Año %d: %d vinos, precio medio: $%0.2f" % (a["key"], a["doc_count"],a["precio-medio-por-año"]["value"]))

