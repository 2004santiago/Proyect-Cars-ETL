from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import logging
import os
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import great_expectations as gx


def load_API_data():
    url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/?frequency=weekly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000&api_key=bqwjaJLDl8NGnarM5gvFz7iDmIGNyKK47vtgmX91"

    # Llamado a la API
    response = requests.get(url)

    # Verifica si el llamado fue exitoso (status code 200)
    if response.status_code == 200:
        # Convierte la respuesta a formato JSON
        data = response.json()
        
        # Extrae los datos relevantes (ajusta si es necesario según la estructura del JSON)
        if 'response' in data and 'data' in data['response']:
            records = data['response']['data']
            
            # Cargar los datos en un DataFrame de pandas
            df = pd.DataFrame(records)
            
            # Muestra las primeras filas del DataFrame
            print(df.head())
            print(f"Total de registros obtenidos: {len(df)}")
        else:
            print("Error: La estructura del JSON no contiene los datos esperados.")
    else:
        print(f"Error: Falló el llamado a la API con el código de estado {response.status_code}")
    
    logging.info("Successfully loaded from API.")

    #save = df.to_csv('Data/Raws/petroleum2.csv', index=False)
    return df.to_json(orient='records')

def load_dataset():
    try:
        load_dotenv()

        localhost = os.getenv('LOCALHOST')
        port = os.getenv('PORT')
        nameDB = os.getenv('DB_NAME')
        userDB = os.getenv('DB_USER')
        passDB = os.getenv('DB_PASS')
        
        engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')
        inspector = inspect(engine)
        
        connection = engine.connect()
        logging.info("Successfully connected to the database.")
        
        dataframe = 'cars'  
        df_cars = pd.read_sql_table(dataframe, engine)
        
        logging.info("Successfully loaded the data.")
        
        logging.info(df_cars.info())
        
        connection.close()
        
        return df_cars.to_json(orient='records')
    
    
    except Exception as e:
        logging.error(f"Error loading the data of Dataset: {str(e)}")
