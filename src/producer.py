import time
import pandas as pd
from kafka import KafkaProducer
from json import dumps

# Configuración de Kafka (directamente en el código)
KAFKA_TOPIC = 'kafka_project'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Función del productor de Kafka con intervalo de envío
def kafka_producer(data, interval=3):
    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )

    for index in range(len(data)):
        # Convertir la fila a diccionario y enviar a Kafka
        row = data.iloc[index].to_dict()
        producer.send(KAFKA_TOPIC, value=row)
        print(f"Mensaje enviado: {row}")
        
        # Esperar el intervalo de tiempo especificado antes de enviar la siguiente fila
        time.sleep(interval)

    producer.flush()  # Asegura que todos los mensajes se envíen antes de cerrar

if __name__ == "__main__":
    # Cargar el archivo CSV y enviar los datos con un intervalo de 3 segundos entre filas
    data = pd.read_csv("../Data/Clean/cars_clean.csv")
    kafka_producer(data, interval=3)