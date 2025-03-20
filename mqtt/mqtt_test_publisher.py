import random
import time
import json
from threading import Thread
from paho.mqtt import client as mqtt_client
from datetime import datetime


broker = "test.mosquitto.org"
port = 1883
base_topic = "parqueoApp"

def Datos(station_id):
    hora_actual = time.localtime()
    fecha_formateada = time.strftime("%Y-%m-%d", hora_actual)
    hora_formateada = time.strftime("%H:%M:%S", hora_actual)
    fecha_hora = datetime.strptime(f"{fecha_formateada} {hora_formateada}", "%Y-%m-%d %H:%M:%S")

    return {
        "estacion_id": station_id,
        "temperatura": round(random.uniform(18, 35), 2),
        "humedad": round(random.uniform(0, 100), 2),
        "presionatmosferica": round(random.uniform(950, 1050), 2),
        "velocidad_del_viento": round(random.uniform(0, 100), 2),
        "direccion_del_viento": round(random.uniform(0, 255), 2),
        "pluvialidad": round(random.uniform(0, 100), 2),
        "hora": fecha_hora
    }

def DatosParqueo(parking_id):
    hora_actual = time.localtime()
    fecha_formateada = time.strftime("%Y-%m-%d", hora_actual)
    hora_formateada = time.strftime("%H:%M:%S", hora_actual)
    fecha_hora = datetime.strptime(f"{fecha_formateada} {hora_formateada}", "%Y-%m-%d %H:%M:%S")

    #actualmente los valores los estamos generando artificialmente 
    parqueos_ocupados = random.randint(0, 20)  
    parqueos_disponibles = random.randint(0, 20)  

    return {
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


# Publicador de datos MQTT
def publish(client, parking_id):
    base_topic = "parqueoApp"
    while True:
        datos = DatosParqueo(parking_id)
        payload = json.dumps(datos, default=str)
        topic = f"{base_topic}/{parking_id}/disponibilidad"
        result = client.publish(topic, payload)
        status = result[0]
        if status == 0:
            print(f"Enviado {payload} a {topic}")
        else:
            print(f"Mensaje fallido {topic}")
        time.sleep(5)

def run_publisher(parking_ids):
    threads = []
    for parking_id in parking_ids:
        client_id = f'publish-{parking_id}-{random.randint(0, 1000)}'
        client = connect_mqtt(client_id)
        t = Thread(target=publish, args=(client, parking_id))
        t.start()
        threads.append(t)
        time.sleep(5)
    for t in threads:
        t.join()

# Ejecución del publicador
if __name__ == '__main__':
    parking_ids = [1, 2]  # IDs de parqueos de ejemplo
    run_publisher(parking_ids)