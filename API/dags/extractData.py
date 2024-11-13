import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
import requests
import great_expectations as gx
from tqdm import tqdm


logging.basicConfig(level=logging.INFO)

def load_API_data_with_expectations():
    url = "https://api.eia.gov/v2/petroleum/pri/gnd/data/?frequency=weekly&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=1000&api_key=bqwjaJLDl8NGnarM5gvFz7iDmIGNyKK47vtgmX91"
    
    # Llamado a la API
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        
        if 'response' in data and 'data' in data['response']:
            records = data['response']['data']
            df = pd.DataFrame(records)
            
            # Crear un contexto temporal para el DataFrame
            context = gx.get_context()
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

            # Añadir expectativas para cada columna sobre tipo y no nulo
            for column, dtype in column_types.items():
                total_expectations += 2  # Expectativas de tipo y no nulo
                
                # Validación de cada expectativa
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

            return df.to_json(orient='records')
        else:
            logging.error("JSON structure does not contain the expected data.")
    else:
        logging.error(f"Failed API call with status code {response.status_code}")

