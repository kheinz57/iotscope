#!/usr/bin/env python3

import re
import paho.mqtt.client as mqtt
import time
from datetime import datetime, timedelta
import json

MQTT_ADDRESS = 'mqtt'
MQTT_USER = ''
MQTT_PASSWORD = ''
MQTT_TOPIC = '/hoizung/#'  
#MQTT_REGEX = 'hoizung/([^/]+)/([^/]+)'
MQTT_CLIENT_ID = ''

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + str(msg.payload))
    if "hoizung/json" in msg.topic:
        print(msg.payload)
        jsonData = json.loads(msg.payload)
        print (jsonData)
        print (jsonData['T'])
        T = jsonData['T']
        #sum = T[0]+T[1]+T[2]+T[3]+T[4]+T[5]+T[6]+T[7]
        sum = (T[18]*0) + (T[19]*20) + (T[20]*20) + (T[17]*20)
        sum = sum + (T[1]*6) + (T[2]*6) + (T[3]*6) + (T[4]*6) + (T[5]*6) + (T[6]*6) + (T[7]*0)
        Tavg = sum/96 | 
        Tmin = 40 # Grad C
        Tmax = 78 # Grad C
        Trange = Tmax - Tmin
        Tnormalized = Tavg - Tmin
        percent = (Tnormalized * 100)/Trange
        print ("version 3: ", sum,Trange, Tnormalized, Tavg, percent)
        if percent > 100: percent = 100
#        percent = (avg - 35) * (100/(85-35))
        client.publish("/hoizung/calc/buffer", payload=str(percent), qos=0, retain=True)
        publishhtml(percent,T)

def getNowStr():
    now = datetime.now() +  + timedelta(hours=1)
    d = now.strftime("%d.%m.%Y  %H:%M:%S")
    return d

def formatTemperatures(T):
    s = str(T).strip('[]')
    s = s.replace(","," Grad <p>") + " Grad"
    return s

def publishhtml(percent,T):
    nowStr = getNowStr()
    print (nowStr)
    zonesStr = formatTemperatures(T[18:21] + T[17:18] + T[0:8])
    print (zonesStr)
    color = "white"
    if percent > 70:
      color = "yellow"
    if percent > 85:
      color = "red"
    page = '<html xmlns="http://www.w3.org/1999/xhtml"><head><meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1"><meta http-equiv="refresh" content="30"><title>Heizung Fuellgrad Pufferspeicher</title></head><body style="background-color:{}" ><div style="font-size:250px">{}%</div><div style="font-size:60px">{}</div><div style="font-size:20px">{}</div></body></html>'.format(color, int(percent),        nowStr,zonesStr)
    print (page)
    with open("/var/www/gug-nemme.de/public/heizung.html","w") as f:
        f.write(page) 


def main():
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.publish("/hoizung/calc/buffer", payload="1000", qos=0, retain=True)

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    #print(getNowStr())
    #print (formatTemperatures([70,34,56,7,8]))
    main()
