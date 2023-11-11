import json
import time
import random
from confluent_kafka import Producer

# Configuration
bootstrap_servers = '157.245.244.105:9092'
topic = f'datos-meteorologicos-20332'

# Configuracion de generadores de datos
temperatura_media = 50.0
humedad_media = 50
varianza_temperatura = 10.0
varianza_humedad = 10

# Configuracion de productor
conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

def generar_datos():
    temperatura = round(random.uniform(temperatura_media - varianza_temperatura, temperatura_media + varianza_temperatura), 2)
    humedad = random.randint(humedad_media - varianza_humedad, humedad_media + varianza_humedad)
    direccion_viento = random.choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])

    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }

def delivery_report(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        print(msg.value())
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))

def enviar_datos():
    while True:
        datos = generar_datos()
        mensaje = json.dumps(datos)
        producer.produce(topic, key="sensor", value=str(mensaje), callback=delivery_report)
        print(mensaje)
        time.sleep(30)  # Enviar datos cada 5 segundos

if __name__ == '__main__':
    enviar_datos()