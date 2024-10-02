import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.INFO)

def load_API_data():
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
        
        dataframe = 'api_petroleum'  
        df_API = pd.read_sql_table(dataframe, engine)
        
        logging.info("Successfully loaded the data.")
        
        logging.info(df_API.info())
        
        connection.close()
        
        return df_API.to_json(orient='records')
    
    
    except Exception as e:
        logging.error(f"Error loading the API data : {str(e)}")
        
        
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