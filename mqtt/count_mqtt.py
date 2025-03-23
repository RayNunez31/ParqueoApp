import random
import torch
import cv2
import numpy as np
import json
from threading import Thread
from paho.mqtt import client as mqtt_client
from datetime import datetime
import time

broker = "test.mosquitto.org"
port = 1883
base_topic= "parqueoApp"

model = torch.hub.load('ultralytics/yolov5', 'custom', path='/home/parqueoApp/parqueoApp/ParqueoApp/yolov5/runs/train/exp24/weights/best.pt', source='github')
model.conf = 0.3
image_path = '/home/parqueoApp/parqueoApp/ParqueoApp/Detect/imagen_prueba6.jpeg'
img = cv2.imread(image_path)
size = 416
results = model(img,size)
detections = results.pandas().xyxy[0]
print(detections)


def Datos(parking_id,detections):
	hora_actual = time.localtime()
	fecha_formateada = time.strftime("%Y-%m-%d", hora_actual)
	hora_formateada = time.strftime("%H:%M:%S",hora_actual)
	fecha_hora = datetime.strptime(f"{fecha_formateada} {hora_formateada}", "%Y-%m-%d %H:%M:%S")

	parqueos_ocupados = 0 
	parqueos_disponibles = 0
	
	for _, row in detections.iterrows():
		label = row['name']
		if label == 'Ocuppied':
			parqueos_ocupados += 1
		elif label == 'Available':
			parqueos_disponibles += 1

	return{
		"parqueo_id": parking_id,
		"parqueos_ocupados": parqueos_ocupados,
		"parqueos_disponibles": parqueos_disponibles,
		"hora": fecha_hora
	}

def connect_mqtt(client_id):
	def on_connect(client, userdata, flags, rc):
		if rc == 0:
			print(f"Conectado al broker {client_id}!")
		else:
			print(f"Conexion fallida {rc}\n")
	
	client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION1)
	client.on_connect = on_connect
	client.connect(broker, port)
	return client

def publish(client, parking_id):
	base_topic= "parqueoApp"
	datos = Datos(parking_id, detections)
	payload = json.dumps(datos, default=str)
	topic = f"{base_topic}/{parking_id}/disponibilidad"
	result = client.publish(topic, payload)
	status = result[0]
	if status == 0:
		print(f"Enviado {payload} a {topic}")
	else:
		print(f"Mensaje fallidos {topic}")
	time.sleep(5)

def run_publisher(parking_ids):
	threads = []
	for parking_id in parking_ids:
		client_id = f'publish-{parking_id}-{random.randint(0,1000)}'
		client  = connect_mqtt(client_id)
		t = Thread(target=publish, args=(client, parking_id))
		t.start()
		threads.append(t)
		time.sleep(5)
	for t in threads:
		t.join()

if __name__ == '__main__':
	parking_ids = [1]
	run_publisher(parking_ids)

