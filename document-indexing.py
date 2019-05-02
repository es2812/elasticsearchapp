"""
    Script de Python 3 que realiza el lanzamiento de peticiones a la API REST
    de Elasticsearch (localhost:9200) para el indexado de los conjuntos de datos
    `inn`, `marathon`, `norte` y `wine`.

    Dependencias: requests, json, os, re (los últimos tres son librerías por defecto en Python3)
"""

import json
import requests
import os
import re

ELASTIC = "http://localhost:9200" #dirección en la que está funcionando elastic

path_script = os.path.dirname(os.path.realpath(__file__)) #permite acceder a los ficheros en path relativo al script

def createIndex(elastic_url,index_name,mapp):
    """
        Crea un nuevo índice con nombre index_name y mapping mapp en elasticsearch (corriendo en elastic_url).
        Si el índice ya existía lo elimina.
    """
    
    res = requests.put(elastic_url+"/"+index_name, json={"mappings":mapp})
    
    if(res.status_code==400):
        # 400 significa error. El índice no se ha creado, normalmente porque ya existía. Eliminamos el índice.
        res = requests.delete(elastic_url+"/"+index_name)
        res = requests.put(elastic_url+"/"+index_name, json={"mappings":mapp})

    if(res.status_code==200):
        print("Index %s creado" % index_name)
        return
    else:
        print("Error en creación de index %s" % index_name)
        return

def uploadBulk(abs_path, elastic_url, index_name):
    """
    Realiza la subida de los ficheros encontrados en abs_path al índice con nombre index_name en elastic corriendo en elastic_url. 
    También realiza algunas modificaciones en inn y maraton (debido al formato de fechas/tiempos).

    Si ya existen documentos en el índice los añade a continuación.
    """
    with open(abs_path) as f:
        obj = json.load(f) #leemos los datos a una lista de diccionarios

    # Construimos los datos a enviar.
    # El formato debe ser, para cada instancia:
    #   - una línea {"index": {} } (con índice en blanco elasticsearch automáticamente crea uno único)
    #   - seguida de una línea con el contenido del documento
    data = ''

    for doc in obj:
        data += '{ "index": {}}\n'
        data += json.dumps(doc)+'\n'

    # En el fichero "inn.json" las fechas se encuentran escritas con el mes en mayúsuclas (p.e. 02-DEC-17). 
    # El DateFormatter de Java no puede leer ese mes correctamente.
    # Utilizamos regex para detectar estos casos y pasar las últimas letras del mes a minúsculas.
    data = re.sub(r'(\d{2})-([A-Z]{3})-(\d{2})',lambda m: m.group(1)+"-"+m.group(2)[0]+m.group(2)[1:].lower()+"-"+m.group(3),data)

    # En el fichero "maraton.json" el campo Pace se encuentra en minutos y segundos, 
    # para poder almacenarlo como fecha se debe añadir la hora delante (0 en este caso)
    data = re.sub(r'"(\d+:\d{2}")',lambda m: '"0:'+m.group(1),data)

    # Subida de los documentos a elastic
    res = requests.post(elastic_url+"/"+index_name+"/_bulk", headers={'Content-Type': 'application/x-ndjson'}, data=data)

    if(res.status_code==200):
        print("%d documentos almacenados a index %s" % (len(obj),index_name))
        return
    else:
        print("Error en almacenamiento de documentos a index %s" % index_name)
        print(res.text)
        return


"""
            WINE DATASET
"""
pathwine = os.path.join(path_script,"data/wine/wine.json")

mapwine = { "properties" : {
    "No" : { "type" : "long" },
    "Year" : { "type" : "integer" },
    "Price" : { "type" : "long" },
    "Score" : { "type" : "integer" }
    }
}

createIndex(ELASTIC,"wine",mapwine)
uploadBulk(pathwine,ELASTIC,"wine")

"""
            INN DATASET
"""

pathinn = os.path.join(path_script,"data/inn/inn.json")

mapinn = { "properties" : {
    "Code" : { "type" : "long" },
    "CheckIn" : { "type" : "date",
                  "format" : "dd-MMM-yy"},
    "CheckOut" : { "type" : "date",
                  "format" : "dd-MMM-yy"},
    "Rate" : { "type" : "double" },
    "Adults" : { "type" : "integer" },
    "Kids" : { "type" : "integer" },
    "Room" : { "properties" : {
        "beds" : { "type" : "integer" },
        "maxOccupancy" : { "type" : "integer" },
        "basePrice" : { "type" : "long" }
    } }
    }
}

createIndex(ELASTIC,"inn",mapinn)
uploadBulk(pathinn,ELASTIC,"inn")

"""
            MARATON DATASET
"""

pathmar = os.path.join(path_script,"data/maraton/maraton.json")

mapmar = { "properties" : {
    "Place" : { "type" : "integer" },
    "Time" : { "type" : "date",
                  "format" : "H:mm:ss"},
    "Pace" : { "type" : "date",
                  "format" : "H:m:ss"},
    "GroupPlace" : { "type" : "integer" },
    "Age" : { "type" : "integer" },
    "BIBNumber": { "type" : "integer" } 
    }
}

createIndex(ELASTIC,"maraton",mapmar)
uploadBulk(pathmar,ELASTIC,"maraton")

# """
#             NORTE DATASET
# """

pathrootnorte = os.path.join(path_script,"data/norte/")

mapnorte = { "properties" : {
    "docid" : { "type" : "long" },
    "fecha" : { "type" : "date",
                  "format" : "dd-mm-yyyy"}
    }
}

createIndex(ELASTIC,"norte",mapnorte)

for dir,_,files in os.walk(pathrootnorte): 
    #los ficheros se encuentran en múltiples directorios, los recorremos todos y lanzamos un bulk 
    # por cada fichero json encontrados
    for file in files:
        uploadBulk(os.path.join(dir,file),ELASTIC,"norte")