import pandas as pd
from src.kafka_module import kafka_producer, kafka_consumer  # Importa ambas funciones

if __name__ == "__main__":
    # Carga el archivo CSV en un DataFrame
    data = pd.read_csv("Data/Clean/cars_clean.csv")
    
    # Llama al producer para enviar los datos al topic de Kafka
    kafka_producer(data)
    
    # Llama al consumer para consumir los datos del topic
    kafka_consumer()