#!/usr/bin/env python
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import os,sys,logging,time
from influxdb import InfluxDBClient
import json

MQTT_ADDRESS = os.getenv("LOCAL_MQTT_SERVER_ADDRESS",'mqtt')
MQTT_PORT = os.getenv("LOCAL_MQTT_SERVER_PORT",'1883')
MQTT_TOPIC = os.getenv("LOCAL_MQTT_TOPIC",'#')  
MQTT_CLIENT_ID = os.getenv("LOCAL_MQTT_CLIENT_ID",'tests')

INFLUXDB_ADDRESS = os.getenv("LOCAL_INFLUXDB_SERVER_ADDRESS",'influxdb')
INFLUXDB_USER = 'root'
INFLUXDB_PASSWORD = ''
INFLUXDB_DATABASE = ''

influxdb_client = InfluxDBClient(INFLUXDB_ADDRESS, 8086, INFLUXDB_USER, INFLUXDB_PASSWORD, None)

def initDB():
    databases = influxdb_client.get_list_database()
    for x in databases:
        print(":i",x['name'])
    if len(list(filter(lambda x: x['name'] == INFLUXDB_DATABASE, databases))) == 0:
        print("create database ", INFLUXDB_DATABASE)
        influxdb_client.create_database(INFLUXDB_DATABASE)
    influxdb_client.switch_database(INFLUXDB_DATABASE)
def send2DB(name,value):
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
    time.sleep(15)
    print ("after sleep")
    printinfo()
    #os.system("ip a")
    os.system("ping " + MQTT_ADDRESS + " -c 2")
    os.system("ping " +  INFLUXDB_ADDRESS + " -c 2")
    initDB()
    main()
