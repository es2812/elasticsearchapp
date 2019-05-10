import json
import requests
from datetime import datetime

ELASTIC = "http://localhost:9200" #dirección en la que está funcionando elastic

"""
    Media, mejor y peor tiempo por grupo de edad:
"""

print("Media, mejor y peor tiempo por grupo de edad:")
print()

query = {
    "size" : 0,
    "aggs" : {
        "tiempos_por_grupo" : {
            "terms" : {
                "field" : "Group.keyword"
            },
            "aggs" : {
                "tiempos" : {
                    "avg" : {
                        "field" : "Time"
                    }
                },
                "mejor-tiempo" : {
                    "min" : {
                        "field" : "Time"
                    }
                },
                "peor-tiempo" : {
                    "max" : {
                        "field" : "Time"
                    }
                }
            }
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/maraton/_search",json=query).text)["aggregations"]["tiempos_por_grupo"]["buckets"]

for r in res:
    
    print("Grupo de edad %s: %d corredores, tiempo medio %s, tiempo perdedor: %s, tiempo ganador: %s" % (r["key"], r["doc_count"], r["tiempos"]["value_as_string"], r["peor-tiempo"]["value_as_string"], r["mejor-tiempo"]["value_as_string"]))

print()
"""
    Mejor posición por cada Estado
"""

print("Mejor posición por cada Estado:")
print()

query = {
    "size" : 0,
    "aggs" : {
        "estados" : {
            "terms" : {
                "field" : "State.keyword"
            },
            "aggs" : {
                "mejor" : {
                    "min" : {
                        "field" : "Place"
                    }
                },
            }
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/maraton/_search",json=query).text)["aggregations"]["estados"]["buckets"]

for r in res:
    print("%s: %d corredores, mejor posición %d" % (r["key"], r["doc_count"], r["mejor"]["value"]))

print()

"""
    Corredores con Pace entre 6 y 8 minutos que hayan quedado por encima del puesto 5 en su grupo
"""

print("Corredores con Pace entre 6 y 8 minutos que hayan quedado por encima del puesto 5 en su grupo:")
print()

query = {
    "size": 1000,
    "query" : {
        "bool" : {
            "must" : [{
                "range" : { 
                    "Pace" : {
                        "gte" : "0:6:00",
                        "lte" : "0:8:00"
                    }
                }},
                {"range" : {
                    "GroupPlace" : {
                        "lte" : 5
                    }
                }
            }]
        }
    }
}


res = json.loads(requests.get(ELASTIC+"/maraton/_search",json=query).text)["hits"]["hits"]

for re in res:
    r = re["_source"]
    print("%s, Pace: %s, Puesto en su grupo(%s) %d" % (r["FirstName"]+" "+r["LasName"], r["Pace"], r["Group"], r["GroupPlace"]))

print()

"""
    Número de BIB de los diez mejores corredores
"""

print("Número de BIB de los diez mejores corredores:")
print()

query = {
    "size": 10,
    "query" : {
        "range" : {
            "Place" : {
                "lte" : "10"
            }
        }
    }
}

res = json.loads(requests.get(ELASTIC+"/maraton/_search",json=query).text)["hits"]["hits"]

for re in res:
    r = re["_source"]
    print("%s, puesto %s, BIB: %s" % (r["FirstName"]+" "+r["LasName"], r["Place"], r["BIBNumber"]))

print()