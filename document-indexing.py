"""
    Script de Python 3 que realiza el lanzamiento de peticiones a la API REST
    de Elasticsearch (localhost:9200) para el indexado de los conjuntos de datos
    `inn`, `marathon`, `norte` y `wine`.

    Dependencias: requests
"""

import json
import requests
import os
import re

ELASTIC = "http://localhost:9200"

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
        print(res.text)

    if(res.status_code==200):
        print("Index %s creado" % index_name)
    else:
        print("Error en creación de index %s" % index_name)

def uploadBulk(abs_path, elastic_url, index_name):
    """
    Realiza la subida de los ficheros encontrados en abs_path al índice con nombre index_name en elastic corriendo en elastic_url
    """

    with open(abs_path) as f:
        obj = json.load(f)

    #construimos los datos a enviar para el _bulk
    data = ''

    for index, doc in enumerate(obj):
        data += '{ "index": {"_id": '+str(index)+'}}\n' #añadimos el índice del documento
        data += json.dumps(doc).replace("},","}")
        data += '\n'
    data += '\n'

    # En el fichero "inn.json" las fechas se encuentran escritas con el mes en mayúsuclas (p.e. 02-DEC-17). el DateFormatter de Java no puede leer esto. 
    # Utilizamos regex para pasar las últimas letras del mes a minúsculas en estos casos
    data = re.sub(r'(\d{2})-([A-Z]{3})-(\d{2})',lambda m: m.group(1)+"-"+m.group(2)[0]+m.group(2)[1:].lower()+"-"+m.group(3),data)

    # En el fichero "maraton.json" el campo Pace se encuentra en minutos y segundos, para poder almacenarlo como fecha se debe añadir la hora delante (0 en este caso)
    data = re.sub(r'"(\d+:\d{2}")',lambda m: '"0:'+m.group(1),data)

    res = requests.post(elastic_url+"/"+index_name+"/_bulk", headers={'Content-Type': 'application/x-ndjson'}, data=data)

    if(res.status_code==200):
        print("%d documentos almacenados a index %s" % (len(obj),index_name))
    else:
        print("Error en almacenamiento de documentos a index %s" % index_name)

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

pathinn = os.path.join(path_script,"data/inn/inn.json")

mapinn = { "properties" : {
    "Code" : { "type" : "long" },
    "CheckIn" : { "type" : "date",
                  "format" : "dd-MMM-yy"},
    "CheckIn" : { "type" : "date",
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

pathrootnorte = os.path.join(path_script,"data/norte/")

mapnorte = { "properties" : {
    "docid" : { "type" : "long" },
    "fecha" : { "type" : "date",
                  "format" : "dd-mm-yyyy"}
    }
}

createIndex(ELASTIC,"norte",mapnorte)

for dir,_,files in os.walk(pathrootnorte):
    for file in files:
        uploadBulk(os.path.join(dir,file),ELASTIC,"norte")