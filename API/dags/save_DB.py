import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.INFO)


def save_data():
    try:
        
        # Definir la ruta de los CSVs (ajusta según tu sistema)
        csv_directory = 'Data/Fact tables'  # Ruta de la carpeta con los CSVs
        
        # Leer los archivos CSV desde la ruta
        dimension_vehiculo = pd.read_csv(os.path.join(csv_directory, 'dimension_vehiculo.csv'))
        dimension_vendedor = pd.read_csv(os.path.join(csv_directory, 'dimension_vendedor.csv'))
        dimension_ratings = pd.read_csv(os.path.join(csv_directory, 'dimension_ratings.csv'))
        fact_table = pd.read_csv(os.path.join(csv_directory, 'tabla_hechos.csv'))
        
        logging.info("Data loaded successfully from CSV files.")
        
        # Cargar las variables de entorno para la conexión a la base de datos
        load_dotenv()

        # Extraer los detalles de la base de datos desde las variables de entorno
        localhost = os.getenv('LOCALHOST')
        port = os.getenv('PORT')
        nameDB = os.getenv('DB_NAME')
        userDB = os.getenv('DB_USER')
        passDB = os.getenv('DB_PASS')

        # Crear la conexión con la base de datos
        engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')
        logging.info("Connected to the database successfully.")

        # Guardar las dimensiones y la tabla de hechos en diferentes tablas en la base de datos
        dimension_vehiculo.to_sql('dimension_vehiculo', con=engine, if_exists='replace', index=False)
        logging.info("Dimension Vehiculo saved to table 'dimension_vehiculo' successfully.")
        
        dimension_vendedor.to_sql('dimension_vendedor', con=engine, if_exists='replace', index=False)
        logging.info("Dimension Vendedor saved to table 'dimension_vendedor' successfully.")
        
        dimension_ratings.to_sql('dimension_ratings', con=engine, if_exists='replace', index=False)
        logging.info("Dimension Ratings saved to table 'dimension_ratings' successfully.")
        
        fact_table.to_sql('fact_table', con=engine, if_exists='replace', index=False)
        logging.info("Fact Table saved to table 'fact_table' successfully.")
        
    except Exception as e:
        logging.error(f"Error saving data to the database: {str(e)}")
        
        