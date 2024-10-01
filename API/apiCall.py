import requests
import pandas as pd

# URL
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

save = df.to_csv('Data/Raws/petroleum2.csv', index=False)
