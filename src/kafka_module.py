import os
import sys
import time
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
from sqlalchemy.orm import sessionmaker
from json import dumps, loads
from dotenv import load_dotenv

# Cargar variables de entorno solo para la configuración de la base de datos
load_dotenv()

# Configurar la conexión a la base de datos desde variables de entorno
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Configuración de la conexión de SQLAlchemy
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Definición de la tabla en SQLAlchemy (ajusta las columnas según tus datos)
metadata = MetaData()
tabla_kafka = Table(
    'cars_data', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('year', Integer),
    Column('make', String),
    Column('model', String),
    Column('used', String),
    Column('price', Integer),
    Column('consumer_rating', Float),
    Column('consumer_reviews', Integer),
    Column('seller_type', String),
    Column('seller_name', String),
    Column('seller_rating', Float),
    Column('interior_color', String),
    Column('drivetrain', String),
    Column('min_mpg', Integer),
    Column('max_mpg', Integer),
    Column('fuel_type', String),
    Column('transmission', String),
    Column('engine', String),
    Column('vin', String),
    Column('stock', String),
    Column('mileage', Integer)
)

# Crear la tabla en la base de datos si no existe
metadata.create_all(engine)

# Configuración de Kafka 
KAFKA_TOPIC = 'kafka_project'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9093']
KAFKA_GROUP_ID = 'my-group-1'

# Función del consumidor de Kafka
def kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        value_deserializer=lambda m: loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        row = message.value  # Datos en formato de diccionario
        # Inserta la fila en la base de datos
        ins = tabla_kafka.insert().values(
            year=row.get('Year'),
            make=row.get('Make'),
            model=row.get('Model'),
            used=row.get('Used'),
            price=row.get('Price'),
            consumer_rating=row.get('ConsumerRating'),
            consumer_reviews=row.get('ConsumerReviews'),
            seller_type=row.get('SellerType'),
            seller_name=row.get('SellerName'),
            seller_rating=row.get('SellerRating'),
            interior_color=row.get('InteriorColor'),
            drivetrain=row.get('Drivetrain'),
            min_mpg=row.get('MinMPG'),
            max_mpg=row.get('MaxMPG'),
            fuel_type=row.get('FuelType'),
            transmission=row.get('Transmission'),
            engine=row.get('Engine'),
            vin=row.get('VIN'),
            stock=row.get('Stock#'),
            mileage=row.get('Mileage')
        )
        conn = engine.connect()
        conn.execute(ins)
        conn.close()
        print(f"Mensaje guardado en la base de datos: {row}")

# Ejecución principal
if __name__ == "__main__":
    kafka_consumer()