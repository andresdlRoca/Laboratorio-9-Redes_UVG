import json
import time
import random
from confluent_kafka import Producer
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Configuración del productor de Kafka
bootstrap_servers = '157.245.244.105:9092'
topic = 'datos-meteorologicos-20332'

temperatura_media = 50.0
humedad_media = 50
varianza_temperatura = 10.0
varianza_humedad = 10

conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

# Función para generar datos meteorológicos
def generar_datos():
    temperatura = round(random.uniform(temperatura_media - varianza_temperatura, temperatura_media + varianza_temperatura), 1)
    humedad = random.randint(humedad_media - varianza_humedad, humedad_media + varianza_humedad)
    direccion_viento = random.choice(["N", "NW", "W", "SW", "S", "SE", "E", "NE"])
    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion_viento
    }

# Funciones para codificar y decodificar mensajes
def encode(temperatura, humedad, direccion_viento):
    direcciones = {"N": 0, "NW": 1, "W": 2, "SW": 3, "S": 4, "SE": 5, "E": 6, "NE": 7}
    
    # Asegurar que la temperatura codificada esté en el rango 0 a 16383
    temp_ajustada = max(min(temperatura, 50), -50)
    temp_codificada = int((temp_ajustada + 50) * 163.83)  # Ajuste para que quepa en 14 bits

    dir_viento_codificada = direcciones[direccion_viento]
    payload = (temp_codificada << 10) | (humedad << 3) | dir_viento_codificada

    # Imprimir detalles de codificación
    print(f"Codificando: Temperatura={temperatura}, Temp. Codificada={temp_codificada}, "
          f"Humedad={humedad}, Dirección Viento={direccion_viento} (Codificada={dir_viento_codificada}), "
          f"Payload (antes de bytes)={payload}")

    try:
        return payload.to_bytes(3, byteorder='big')
    except OverflowError:
        print(f"Error: valor de payload demasiado grande para convertir: {payload}")
        return None


def decode(payload_bytes):
    payload = int.from_bytes(payload_bytes, byteorder='big')
    temp_codificada = (payload >> 10) & 0x3FFF  # 14 bits para la temperatura
    temperatura = (temp_codificada / 163.84) - 50
    humedad = (payload >> 3) & 0x7F  # 7 bits para la humedad
    direccion_viento = payload & 0x07  # 3 bits para la dirección del viento
    direcciones = ["N", "NW", "W", "SW", "S", "SE", "E", "NE"]
    direccion_viento_str = direcciones[direccion_viento]
    print(f"Decodificación: Temp={temperatura}, Hum={humedad}, Dir={direccion_viento_str}")
    return temperatura, humedad, direccion_viento_str

# Función de reporte de entrega para el productor
def delivery_report(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        print('Mensaje entregado a {} [{}]'.format(msg.topic(), msg.partition()))

# Función para enviar datos
def enviar_datos():
    while True:
        datos = generar_datos()
        mensaje_codificado = encode(datos['temperatura'], datos['humedad'], datos['direccion_viento'])
        
        if mensaje_codificado is not None:
            producer.produce(topic, key="sensor", value=mensaje_codificado, callback=delivery_report)
            producer.flush()
        else:
            print("No se pudo codificar el mensaje.")
        
        time.sleep(30)

# Configuración del consumidor de Kafka
consumer = KafkaConsumer(
    topic,
    group_id='grupo_consumidores',
    bootstrap_servers=[bootstrap_servers]
)

# Listas para almacenar los datos
all_temp = []
all_hume = []
all_wind = []

# Inicialización del gráfico
plt.ion()
fig, axs = plt.subplots(3, 1, figsize=(10, 6))
line_temp, = axs[0].plot([], [], label='Temperatura')
line_hume, = axs[1].plot([], [], label='Humedad')
line_wind, = axs[2].plot([], [], label='Dirección Viento')
for ax in axs:
    ax.legend()
    ax.set_autoscaley_on(True)
    ax.set_autoscalex_on(True)

# Función para actualizar los datos del gráfico
def plotAllData(temperaturas, humedades, direcciones_viento):
    line_temp.set_data(range(len(temperaturas)), temperaturas)
    line_hume.set_data(range(len(humedades)), humedades)
    line_wind.set_data(range(len(direcciones_viento)), direcciones_viento)
    for ax in axs:
        ax.relim()
        ax.autoscale_view()
    fig.canvas.draw()
    fig.canvas.flush_events()

def direccion_viento_a_numero(direccion_viento):
    direcciones = {"N": 0, "NE": 1, "E": 2, "SE": 3, "S": 4, "SW": 5, "W": 6, "NW": 7}
    return direcciones.get(direccion_viento, -1)


# Función principal
if __name__ == '__main__':
    # Iniciar el productor en un hilo separado
    from threading import Thread
    thread_productor = Thread(target=enviar_datos)
    thread_productor.start()

    # Bucle principal del consumidor para escuchar mensajes y procesarlos
    for mensaje in consumer:
        if mensaje.value is not None:
            payload = decode(mensaje.value)
            all_temp.append(payload[0])
            all_hume.append(payload[1])
            all_wind.append(direccion_viento_a_numero(payload[2]))
            plotAllData(all_temp, all_hume, all_wind)
        else:
            print("Mensaje recibido sin payload.")