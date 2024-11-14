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


def load_API_data(**kwargs):
    url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/?frequency=weekly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000&api_key=bqwjaJLDl8NGnarM5gvFz7iDmIGNyKK47vtgmX91"
    
    # Llamada a la API
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        
        if 'response' in data and 'data' in data['response']:
            records = data['response']['data']
            df = pd.DataFrame(records)
            logging.info("Data successfully extracted from API.")
            
            # Pasar los datos a XCom
            kwargs['ti'].xcom_push(key='api_data', value=df.to_json())
            return df
        else:
            logging.error("JSON structure does not contain the expected data.")
            raise ValueError("Invalid JSON structure")
    else:
        logging.error(f"Failed API call with status code {response.status_code}")
        raise Exception("API call failed")

def validate_api(**kwargs):
    # Obtener los datos de XCom
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='load_data', key='api_data')
    df = pd.read_json(df_json)  # Convertir de JSON a DataFrame
    
    # Definir columnas esperadas
    cols = ["period", "series", "area-name", "product", "product-name", "process", "process-name", "series-description", "value", "duoarea", "units"]
    
    # Configurar y validar expectativas directamente
    validator = gx.from_pandas(df)
    
    # Expectativa: columnas esperadas
    validator.expect_table_columns_to_match_set(column_set=cols, exact_match=False)
    
    # Definir tipos de datos esperados para cada columna
    column_types = {
        "period": "object",  # Ajusta el tipo según tus datos
        "area-name": "object",
        "product": "object",
        "product-name": "object",
        "process": "object",
        "process-name": "object",
        "series-description": "object",
        "value": "object",
        "duoarea": "object",
        "units": "object",
        "series": "object"
    }
    
    # Inicializar contadores para expectativas exitosas y totales
    total_expectations = 0
    successful_expectations = 0

    # Añadir expectativas para cada columna sobre tipo y no nulo, excepto para 'value'
    for column, dtype in column_types.items():
        # Excluir la columna 'value' de la validación de nulos
        if column == "value":
            total_expectations += 1  # Solo validamos el tipo de la columna 'value'
            type_check = validator.expect_column_values_to_be_of_type(column, dtype)
            if type_check.success:
                successful_expectations += 1
        else:
            total_expectations += 2  # Expectativas de tipo y no nulo para las demás columnas

            # Validación de tipo y nulos
            type_check = validator.expect_column_values_to_be_of_type(column, dtype)
            null_check = validator.expect_column_values_to_not_be_null(column)

            # Si cada expectativa pasa, aumenta el contador
            if type_check.success:
                successful_expectations += 1
            if null_check.success:
                successful_expectations += 1
    
    # Calcular porcentaje de expectativas exitosas
    success_percentage = (successful_expectations / total_expectations) * 100

    # Mostrar resultados
    if success_percentage == 100:
        logging.info("Todas las expectativas se validaron correctamente.")
    else:
        logging.warning(f"{success_percentage:.2f}% de expectativas exitosas.")

    # Convertir el resultado a un diccionario JSON serializable
    result_dict = validator.validate().to_json_dict()

    # Mostrar los resultados de validación en un formato legible
    print(json.dumps(result_dict, indent=4))
    print(f"Porcentaje de expectativas exitosas: {success_percentage:.2f}%")



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
