"""
    Script de Python 3 que realiza el lanzamiento de peticiones a la API REST
    de Elasticsearch (localhost:9200) para el indexado de los conjuntos de datos
    `inn`, `marathon`, `norte` y `wine`.

    Dependencias: requests
"""

import json
import requests
import os

path = "data/wine/wine.json"

with open(path) as f:
    wine = json.load(f)

print("Wine:")
print("---------------------")
print("Instancias: "+str(len(wine)))
print("Máximo número de cabeceras: "+str(max(map(lambda x: len(x.keys()),wine))))

print()

path = "data/inn/inn.json"

with open(path) as f:
    inn = json.load(f)

print("Inn:")
print("---------------------")
print("Instancias: "+str(len(inn)))
print("Máximo número de cabeceras: "+str(max(map(lambda x: len(x.keys()),inn))))

print()

path = "data/maraton/maraton.json"

with open(path) as f:
    maraton = json.load(f)

print("Maraton:")
print("---------------------")
print("Instancias: "+str(len(maraton)))
print("Máximo número de cabeceras: "+str(max(map(lambda x: len(x.keys()),maraton))))

print()

path="data/norte/"

norte = list()
for dir,_,files in os.walk(path):
    for file in files:
        with open(os.path.join(dir,file)) as f:
            norte.extend(json.load(f))

print("Norte de Castilla:")
print("---------------------")
print("Instancias: "+str(len(norte)))
print("Máximo número de cabeceras: "+str(max(map(lambda x: len(x.keys()),norte))))
