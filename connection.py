import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

localhost = os.getenv('DB_HOST')
port = os.getenv('PORT')
nameDB = os.getenv('DB_NAME')
userDB = os.getenv('DB_USER')
passDB = os.getenv('DB_PASS')


location_file = 'Data/cars_clean.csv'  
clean_table_database = 'cars'  

engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')

try:
    df = pd.read_csv(location_file, sep=",")

    df.to_sql(clean_table_database, engine, if_exists='replace', index=False)

    print(f"Tabla '{clean_table_database}' creada y datos subidos exitosamente.")

except Exception as e:
    print(f"Error al subir los datos: {e}")

finally:
    engine.dispose()
