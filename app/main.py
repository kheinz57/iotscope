#!/usr/bin/env python
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import os,sys,logging,time
from influxdb import InfluxDBClient
import json

MQTT_ADDRESS = os.getenv("H1_MQTT_SERVER_ADDRESS",'mqtt')
MQTT_PORT = os.getenv("H1_MQTT_SERVER_PORT",'1883')
MQTT_TOPIC = os.getenv("H1_MQTT_TOPIC",'#')  
MQTT_CLIENT_ID = os.getenv("H1_MQTT_CLIENT_ID",'tests')

INFLUXDB_ADDRESS = os.getenv("H1_INFLUXDB_SERVER_ADDRESS",'influxdb')
INFLUXDB_USER = 'root'
INFLUXDB_PASSWORD = 'root'
INFLUXDB_DATABASE = 'hoizung'
influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

def initDB():
    print("before sleep")
    #time.sleep(15)
    print("slept well")
    databases = influxdb_client.get_list_database()
    for x in databases:
        print(":i",x['name'])
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        print("create database ", INFLUXDB_DATABASE)
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)

def convert2Celsius(value):
    if value > 32000:
      value = 0 - (65536 - value)
    value = float(value)/10
    return value


print (convert2Celsius(535))
print (convert2Celsius(65500))
print (convert2Celsius(65535))

def send2DB(name,value):
    if name  == "/hoizung/modbus/json" :
      print (value)
      jsonData = json.loads(value)
      mb = jsonData['MB']
      json_body = [{
        'measurement': 'modbus',
        'fields':{
           'T-Aussen': convert2Celsius(mb[0]),
           'T-PufferOben' : float(mb[1])/10,
           'T-PufferMitte' : float(mb[2])/10,
           'T-PufferUnten' : float(mb[3])/10,
           'PufferLadezustand' : int(mb[4]/10),
           'T-Warmwasser' : float(mb[5])/10,
           'MischerProzent' : float(mb[6])/10,
           'MischerPos' : float(mb[7])/10,
           'T-Vorlauf' : float(mb[8])/10,
           'BA-HolzLadepumpe' :mb[9]-1020,
           'B-HolzLadepumpe' : mb[10]-1040,
           'T-OelBrenner' : float(mb[11])/10,
           'BA-OelBrenner' : mb[12]-1802,
           'B-WWLadePumpe' :mb[13]-1040,
           't-OelRestZeitInSec' :mb[14],
           'BU-HolzUmschaltVentilAktiv' : mb[15]-1040,
           'BA-Brenner' : mb[16]-1020,
           'BA-OelLadepumpe' : mb[17]-1040,
           'HolzKessel' : mb[18]
         }
      }]
    else:
      json_body = [{
         'measurement': name,
         'fields': {
           'value': value 
          }
        }]
    print (json_body)
    try:
      print (influxdb_client.write_points(json_body))
    except Exception as e:
      print(e)
        
def on_connect(client, userdata, flags, rc):
    print ("connected")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    print(getNowStr() + ";" + msg.topic + ';' + str(msg.payload))
    #if msg.topic == "/hoizung/modbus/json" :
    #  print (msg.payload)
    #  jsonData = json.loads(msg.payload)
    send2DB(msg.topic,msg.payload)
    sys.stdout.flush()

def getNowStr():
    now = datetime.now() + timedelta(hours=1)
    d = now.strftime("%d.%m.%Y_%H:%M:%S")
    return d

def printinfo():
    print("MQTT_ADDRESS   = ",MQTT_ADDRESS)
    print("MQTT_PORT      = ",MQTT_PORT)
    print("MQTT_TOPIC     = ", MQTT_TOPIC) 
    print("MQTT_CLIENT_ID = ",MQTT_CLIENT_ID) 

def mainloop():
   while True:  
       sys.stderr.write('Error\n')  
       sys.stdout.write('All Good\n')  
       time.sleep(1)

def main():
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set('','')
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_ADDRESS, int(MQTT_PORT))
    mqtt_client.loop_forever()

if __name__ == '__main__':
    printinfo()
    #os.system("ip a")
    os.system("ping " + MQTT_ADDRESS + " -c 2")
    os.system("ping " +  INFLUXDB_ADDRESS + " -c 2")
    initDB()
    main()
