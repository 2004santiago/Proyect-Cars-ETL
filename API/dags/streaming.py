import time
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import json
import logging

logging.basicConfig(level=logging.INFO)

# Configuración de Kafka 
KAFKA_TOPIC = 'kafka_project'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Función del productor de Kafka con intervalo de envío
def kafka_producer(**kwargs):
    
    ti = kwargs['ti']
    json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
    data = pd.json_normalize(data=json_data)
    logging.info(f"Data received")
    
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
        time.sleep(0.1)

    producer.flush() 