import mysql.connector
import requests
import urllib
import json
import os

#base de datos
conn = None
cursor = None
conn = mysql.connector.connect(
       host="",
       user="",
       password="",
       database=""
       )
print("conexion a la base de datos realizada")

#json data
url =  'http://dadesobertes.seu-e.cat/api/action/datastore_search?resource_id=e0be5678-0bdd-48e0-99af-05cd5404a9a5&filters={"CODI_ENS":"830150006"}'

f = urllib.request.urlopen(url)
myfile = f.read()

writeFileObj = open('output.xml', 'wb')
writeFileObj.write(myfile)
writeFileObj.close()

with open('output.xml', 'r') as f:
  data = json.load(f)

#mapeo campos
v_poblacion = data['result']['records'][0]['TOTAL']
v_year = data['result']['records'][0]['ANY']
v_municipio = 'viladecans'
v_superficie = '20400'

#inserto datos en la base de datos
mycursor = conn.cursor(dictionary=True)
sql = "INSERT INTO z_heritage_opendata_head (fecha_lectura, year, poblacion, municipio, superficie) VALUES (now(), %s, %s, %s, %s)"
val = (v_year, v_poblacion, v_municipio, v_superficie)
print(v_year)
#exit()
mycursor.execute(sql, val)
conn.commit()

#log
print(json.dumps(data, indent=4))
print('----')
print('Poblacion:',data['result']['records'][0]['TOTAL'])
print('AÃ±o:', data['result']['records'][0]['ANY'])


#cierror conexion DB
conn.close()
