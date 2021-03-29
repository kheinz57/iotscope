#!/usr/bin/env python3

import re
import paho.mqtt.client as mqtt
import time
from datetime import datetime, timedelta
import json

import asyncio
from sys import argv
from h1sml import SmlSequence as SmlSequence
from h1sml import SmlGetListResponse as SmlGetListResponse
from h1asyncio import SmlProtocol as SmlProtocol


MQTT_ADDRESS = 'mqtt'
MQTT_USER = ''
MQTT_PASSWORD = ''
MQTT_TOPIC = '/sh/#'  
MQTT_CLIENT_ID = ''

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

class SmlEvent:
    def __init__(self):
      self._cache = {}
      self.mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
      self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
      self.mqtt_client.on_connect = on_connect
#      self.mqtt_client.on_message = on_message
      self.mqtt_client.publish("/test/topic02", payload="1000", qos=0, retain=True)
      self.mqtt_client.connect(MQTT_ADDRESS, 1883)

    def event(self, message_body: SmlSequence) -> None:
        assert isinstance(message_body, SmlGetListResponse)
        for val in message_body.get('valList', []):
            name = val.get('objName')
            value = val.get('value')
            print (name, " = ",value, " ", val.get('unit'))
            #print(val)
            if name:
              if self._cache.get(name) != val:
                self._cache[name] = val
                if "1-0:1.8.0" in name:
                  print ("verbrauch=",value) 
                  self.mqtt_client.publish("/sh/meter/wh", payload=str(value), qos=0, retain=True)
                elif "1-0:16.7.0*255" in name:
                  print ("leistung=",value) 
                  self.mqtt_client.publish("/sh/meter/w", payload=str(value), qos=0, retain=True)
                elif "1-0:2.8.0" in name:
                  print ("Einspeisung=",value) 
                  self.mqtt_client.publish("/sh/meter/nwh", payload=str(value), qos=0, retain=True)

def run(url):
    handler = SmlEvent()
    proto = SmlProtocol(url)
    proto.add_listener(handler.event, ['SmlGetListResponse'])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(proto.connect(loop))
    loop.run_forever()

if __name__ == '__main__':
  print (" starting app")
#    time.sleep(15)
  print ("after sleep")
  run("/dev/ttyUSB0")
