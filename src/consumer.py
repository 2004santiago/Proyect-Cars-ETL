import os
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from json import loads
from dotenv import load_dotenv
import pandas as pd

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de la conexión a la base de datos usando variables de entorno
localhost = os.getenv('LOCALHOST')
port = os.getenv('PORT')
nameDB = os.getenv('DB_NAME')
userDB = os.getenv('DB_USER')
passDB = os.getenv('DB_PASS')

# Crear el motor de SQLAlchemy para conectarse a PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')

# Configuración de Kafka
KAFKA_TOPIC = 'kafka_project'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_GROUP_ID = 'my-group-1'

# Función del consumidor de Kafka
def kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=True,
        value_deserializer=lambda m: loads(m.decode('utf-8'))
    )

    # Consumir mensajes y almacenarlos en la base de datos
    for message in consumer:
        row = message.value  # Datos en formato de diccionario
        
        # Insertar el mensaje de Kafka en la base de datos
        try:
            # Convertir el diccionario en un DataFrame de una sola fila
            df = pd.DataFrame([row])
            
            # Insertar los datos en la base de datos
            df.to_sql('cars_data', engine, if_exists='append', index=False)
            
            print(f"Mensaje guardado en la base de datos: {row}")

        except SQLAlchemyError as e:
            print(f"Error al subir los datos: {e}")

if __name__ == "__main__":
    kafka_consumer()