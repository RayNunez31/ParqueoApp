import json
import psycopg2
import random
from threading import Thread
from paho.mqtt import client as mqtt_client

# Configuración
BROKER = "broker.hivemq.com"
PORT = 1883
BASE_TOPIC = "parqueoApp"
DB_CONFIG = {
    "host": "aws-0-us-east-1.pooler.supabase.com",
    "user": "postgres.achqpmaxjrrrbhwsykqj",
    "password": "postgres",
    "dbname": "postgres",
    "port": 6543
}

# Conectar a PostgreSQL
def connect_db():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"❌Error al conectar con la base de datos: {e}")
        return None

# Crear tablas en el orden correcto
def setup_database():
    connection = connect_db()
    if connection:
        with connection:
            with connection.cursor() as cursor:
                # CREAR TABLA PARQUEO PRIMERO

                # 🔄 REFRESCAR ESQUEMA PARA QUE PostgreSQL RECONOZCA 'parqueo_id'
                cursor.execute("SELECT * FROM parqueo LIMIT 1;")

        connection.close()

#  Conectar a MQTT
def connect_mqtt(client_id):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f" Conectado al broker con ID {client_id}!")
        else:
            print(f"❌ Fallo en la conexión, código {rc}")

    client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION1)
    client.on_connect = on_connect
    client.connect(BROKER, PORT)
    return client

#  Manejo de mensajes MQTT
def on_message(client, userdata, msg):
    print(f" Recibido {msg.payload.decode()} de {msg.topic}")
    try:
        datos = json.loads(msg.payload.decode())
        mapped_data = {
            "parqueo_id": datos.get("parqueo_id"),
            "parqueos_ocupados": datos.get("parqueos_ocupados"),
            "parqueos_disponibles": datos.get("parqueos_disponibles"),
            "hora": datos.get("hora"),
            "parqueo_nombre": datos.get("parqueo_nombre")
        }

        # Insertar datos en la base de datos
        connection = connect_db()
        if connection:
            with connection:
                with connection.cursor() as cursor:
                    insert_query = """
                    INSERT INTO lectura (parqueo_id, parqueos_ocupados, parqueos_disponibles, hora, parqueo_nombre)
                    VALUES (%(parqueo_id)s, %(parqueos_ocupados)s, %(parqueos_disponibles)s, %(hora)s, %(parqueo_nombre)s)
                    """
                    cursor.execute(insert_query, mapped_data)
                    print(" Datos insertados correctamente en la base de datos.")
            connection.close()
    except Exception as e:
        print(f"❌ Error procesando mensaje MQTT: {e}")

#  Suscripción a tópicos MQTT
def subscribe(client, station_ids):
    for station_id in station_ids:
        topic = f"{BASE_TOPIC}/{station_id}/disponibilidad"
        client.subscribe(topic)
        client.message_callback_add(topic, on_message)
    print(" Suscripción completada.")

# 🏃 Hilo del subscriber MQTT
def run_subscriber(station_ids):
    client_id = f'subscriber-{random.randint(0, 1000)}'
    client = connect_mqtt(client_id)
    subscribe(client, station_ids)
    client.loop_forever()

#  Configuración inicial
if __name__ == "__main__":
    try:
        #  Crear las tablas si no existen
        setup_database()

        #  Insertar parqueos de ejemplo si no existen
        connection = connect_db()
        if not connection:
            raise Exception("❌ No se pudo establecer conexión con la base de datos.")

        with connection:
            with connection.cursor() as cursor:
                estaciones = [
                    ('Ing Mecanica', 'Parqueo en frente de laboratorios de ingeniería mecánica')
                ]

                for nombre, descripcion in estaciones:
                    cursor.execute("SELECT parqueo_id FROM parqueo WHERE nombre = %s", (nombre,))
                    station_exists = cursor.fetchone()
                    
                    if station_exists:
                        print(f" Parqueo '{nombre}' ya existe con ID {station_exists[0]}.")
                    else:
                        cursor.execute("INSERT INTO parqueo (nombre, descripcion) VALUES (%s, %s) RETURNING parqueo_id",
                                       (nombre, descripcion))
                        estacion_id = cursor.fetchone()[0]
                        print(f" Parqueo '{nombre}' insertado con ID {estacion_id}.")

                cursor.execute("SELECT parqueo_id FROM parqueo")
                estaciones = cursor.fetchall()
                station_ids = [estacion[0] for estacion in estaciones]

        connection.close()
        print(" Estaciones verificadas e insertadas correctamente.")

        # Iniciar el suscriptor en un hilo
        Thread(target=run_subscriber, args=(station_ids,)).start()

    except Exception as e:
        print(f"❌ Error en el setup: {e}")
