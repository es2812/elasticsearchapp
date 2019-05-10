"""
    Script de Python 3 que realiza consultas a la API REST de Elasticsearch (localhost:9200)
    sobre el índices ya creado: `inn`.

    Dependencias: requests, json, datetime
"""

import json
import requests

from datetime import datetime

ELASTIC = "http://localhost:9200" #dirección en la que está funcionando elastic

"""
    Reservas de duración mayor o igual a 7 días
"""

print("Reservas de duración igual o superior a una semana:")
print("")

query = {
    "size" : 1000,
    "query":
    {
        "script" : {
            "script" : {
                "source": "return (doc['CheckOut'].value.getMillis() - doc['CheckIn'].value.getMillis())/(1000*60*60*24) >= 7"
            }
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/inn/_search",json=query).text)["hits"]["hits"]

for r in res:
    reservation = r["_source"]
    ckin = datetime.strptime(reservation["CheckIn"], "%d-%b-%y")
    ckout = datetime.strptime(reservation["CheckOut"], "%d-%b-%y")

    print("%s %s: Entró %s, Salió %s, duración = %d días" % (reservation["FirstName"], reservation["LastName"],reservation["CheckIn"],reservation["CheckOut"],(ckout-ckin).days))

print("")


"""
    Personas que se han alojado más de una vez
"""

print("Personas que se han alojado más de una vez:")
print("")

query = {
    "size":0,
    "aggs": {
        "persona":{
            "terms" : {
                "script": "params['_source'].FirstName+' '+params['_source'].LastName"
            },
            "aggs":{
                "reservas":{
                    "bucket_selector": {
                        "buckets_path": {
                            "cuenta" : "_count"
                        },
                        "script": {
                            "source" : "params.cuenta > 1"
                        }
                    }
                }
            }
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/inn/_search",json=query).text)["aggregations"]["persona"]["buckets"]

for r in res:
    print("%s se ha alojado %d veces" % (r["key"], r["doc_count"]))

print("")
"""
    Número de reservas al mes para camas de tipo Queen
"""

print("Número de reservas al mes para camas de tipo Queen:")
print("")

query = {
    "size" : 0,
    "query" : {
        "term" : { "Room.bedType" : "queen" }
    },
    "aggs" : {
        "reservas-al-mes" : {
            "terms" : { 
                "script" : "doc['CheckIn'].value.getMonth()"
            }
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/inn/_search",json=query).text)["aggregations"]["reservas-al-mes"]["buckets"]

for r in res:
    print("En el mes %s: %d reservas de camas de tipo Queen" % (r["key"],r["doc_count"]))

print("")
"""
    Precio total de todas las reservas realizadas por EMERY VOLANTE
"""

query = {
    "query" : {
        "bool" : {
            "must" : [{"match" : { "FirstName" : "EMERY"}}, {"match" : { "LastName" : "VOLANTE"}}]
        }
    },
    "aggs" : {
       "precio-total" : {
           "sum" : {
               "script" : {
                   "source" : "(doc.CheckOut.value.getMillis() - doc.CheckIn.value.getMillis())/(1000*60*60*24) * doc.Rate.value" 
               }
           }
       } 
    }
}

res = json.loads(requests.get(ELASTIC+"/inn/_search",json=query).text)["aggregations"]

print("Precio total de todas las reservas realizadas por EMERY VOLANTE: $%0.2f" % res["precio-total"]["value"])
print("")