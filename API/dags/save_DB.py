import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
import shutil

logging.basicConfig(level=logging.INFO)


def save_data():
    try:
        csv_directory = 'Data/Fact_tables'  # Ruta corregida a Fact_tables
        
        # Cargar los archivos CSV
        dimension_vehiculo = pd.read_csv(os.path.join(csv_directory, 'dimension_vehiculo.csv'))
        dimension_vendedor = pd.read_csv(os.path.join(csv_directory, 'dimension_vendedor.csv'))
        dimension_ratings = pd.read_csv(os.path.join(csv_directory, 'dimension_ratings.csv'))
        fact_table = pd.read_csv(os.path.join(csv_directory, 'fact_table.csv'))
        area_dim = pd.read_csv(os.path.join(csv_directory, 'area_dim.csv'))
        product_dim = pd.read_csv(os.path.join(csv_directory, 'product_dim.csv'))
        details_dim = pd.read_csv(os.path.join(csv_directory, 'details_dim.csv'))
        fuel_fact = pd.read_csv(os.path.join(csv_directory, 'fuel_fact.csv'))
        
        logging.info("Data loaded successfully from CSV files.")
        
        # Cargar las variables de entorno para la conexión a la base de datos
        load_dotenv()

        localhost = os.getenv('LOCALHOST')
        port = os.getenv('PORT')
        nameDB = os.getenv('DB_NAME')
        userDB = os.getenv('DB_USER')
        passDB = os.getenv('DB_PASS')

        # Crear la conexión con la base de datos
        engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')
        logging.info("Connected to the database successfully.")

        # Guardar las tablas en la base de datos
        dimension_vehiculo.to_sql('dimension_vehiculo', con=engine, if_exists='replace', index=False)
        logging.info("Dimension Vehiculo saved to table 'dimension_vehiculo' successfully.")
        
        dimension_vendedor.to_sql('dimension_vendedor', con=engine, if_exists='replace', index=False)
        logging.info("Dimension Vendedor saved to table 'dimension_vendedor' successfully.")
        
        dimension_ratings.to_sql('dimension_ratings', con=engine, if_exists='replace', index=False)
        logging.info("Dimension Ratings saved to table 'dimension_ratings' successfully.")
        
        fact_table.to_sql('fact_table', con=engine, if_exists='replace', index=False)
        logging.info("Fact Table saved to table 'fact_table' successfully.")
        
        area_dim.to_sql('area_dim', con=engine, if_exists='replace', index=False)
        logging.info("Area Dimension saved to table 'area_dim' successfully.")
        
        product_dim.to_sql('product_dim', con=engine, if_exists='replace', index=False)
        logging.info("Product Dimension saved to table 'product_dim' successfully.")
        
        details_dim.to_sql('details_dim', con=engine, if_exists='replace', index=False)
        logging.info("Details Dimension saved to table 'details_dim' successfully.")
        
        fuel_fact.to_sql('fuel_fact', con=engine, if_exists='replace', index=False)
        logging.info("Fuel Fact Table saved to table 'fuel_fact' successfully.")
        
        # Eliminar el directorio con los CSVs
        shutil.rmtree(csv_directory)
        logging.info(f"Deleted directory: {csv_directory}")
        
    except Exception as e:
        logging.error(f"Error saving data to the database: {str(e)}")


        
        