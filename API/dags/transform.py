import pandas as pd
import logging
import os
import json
import numpy as np


logging.basicConfig(level=logging.INFO)
        
        
###########################################################################################################################
###########################################################################################################################


def process_data(**kwargs):
    try:
        ti = kwargs['ti']
        
        # Extraer y normalizar los datos del dataset
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
        logging.info("Data DB loaded successfully")
        
        # Extraer y normalizar los datos de la API
        api_data = json.loads(ti.xcom_pull(task_ids="clean_API"))
        apidf = pd.json_normalize(data=api_data)
        logging.info("Data API loaded successfully")
        
        logging.info("Data loaded successfully")

        # Vehículo
        dimension_vehiculo = df[['Year', 'Make', 'Model', 'Drivetrain', 'MinMPG', 'MaxMPG', 
                                 'FuelType', 'Transmission', 'Engine']].drop_duplicates().reset_index(drop=True)
        dimension_vehiculo['ID_Vehiculo'] = dimension_vehiculo.index + 1
        logging.info("Dimension Vehículo created")
        
        # Vendedor
        dimension_vendedor = df[['SellerName', 'SellerType', 'State', 'Zipcode', 'StreetName']].drop_duplicates().reset_index(drop=True)
        dimension_vendedor['ID_Seller'] = dimension_vendedor.index + 1
        logging.info("Dimension Vendedor created")
        
        # Ratings
        dimension_ratings = df[['ConsumerRating', 'SellerRating', 'ComfortRating', 'InteriorDesignRating', 
                                'PerformanceRating', 'ValueForMoneyRating', 'ExteriorStylingRating', 
                                'ReliabilityRating', 'DealType']].drop_duplicates().reset_index(drop=True)
        dimension_ratings['ID_Rating'] = dimension_ratings.index + 1
        logging.info("Dimension Ratings created")

        # Fact Table
        df_hechos_vendedor = pd.merge(df, dimension_vendedor, on=['SellerName', 'SellerType', 'State', 'Zipcode', 'StreetName'], how='left')
        df_hechos_vehiculo = pd.merge(df, dimension_vehiculo, on=['Year', 'Make', 'Model', 'Drivetrain', 'MinMPG', 'MaxMPG', 
                                                                  'FuelType', 'Transmission', 'Engine'], how='left')
        df_hechos_ratings = pd.merge(df, dimension_ratings, on=['ConsumerRating', 'SellerRating', 'ComfortRating', 
                                                                'InteriorDesignRating', 'PerformanceRating', 
                                                                'ValueForMoneyRating', 'ExteriorStylingRating', 
                                                                'ReliabilityRating', 'DealType'], how='left')

        tabla_hechos = df_hechos_vendedor[['Price', 'Mileage', 'ExteriorColor', 'InteriorColor', 'Used', 'VIN', 'Stock#', 'ID_Seller']].copy()
        tabla_hechos['ID_Vehiculo'] = df_hechos_vehiculo['ID_Vehiculo']
        tabla_hechos['ID_Rating'] = df_hechos_ratings['ID_Rating']
        tabla_hechos['ID_Venta'] = df.index + 1
        tabla_hechos = tabla_hechos[['ID_Venta', 'ID_Vehiculo', 'ID_Seller', 'ID_Rating', 'Price', 
                                     'Mileage', 'ExteriorColor', 'InteriorColor', 'Used', 'VIN', 'Stock#']]
        logging.info("Fact Table created")
        
        # Área
        area_dim = apidf[['area', 'area-name']].drop_duplicates().reset_index(drop=True)
        area_dim['area_ID'] = area_dim.index + 1
        logging.info("Area dimension created")
        
        # Producto
        product_dim = apidf[['product', 'product-name']].drop_duplicates().reset_index(drop=True)
        product_dim['product_ID'] = product_dim.index + 1
        logging.info("Product dimension created")

        # Detalles
        details_dim = apidf[['process', 'process-name', 'series-description']].drop_duplicates().reset_index(drop=True)
        details_dim['details_ID'] = details_dim.index + 1
        logging.info("Details dimension created")
        
        # Fuel Fact Table
        df_fuel_area = pd.merge(apidf, area_dim, on=['area', 'area-name'], how='left')
        df_fuel_product = pd.merge(apidf, product_dim, on=['product', 'product-name'], how='left')
        df_fuel_details = pd.merge(apidf, details_dim, on=['process', 'process-name', 'series-description'], how='left')

        fuel_fact = df_fuel_area[['period', 'value($/GAL)']].copy()
        fuel_fact['area_ID'] = df_fuel_area['area_ID']
        fuel_fact['product_ID'] = df_fuel_product['product_ID']
        fuel_fact['details_ID'] = df_fuel_details['details_ID']
        fuel_fact['fuel_ID'] = fuel_fact.index + 1
        fuel_fact = fuel_fact[['fuel_ID', 'period', 'area_ID', 'product_ID', 'details_ID', 'value($/GAL)']]
        logging.info("Fuel Fact Table created")
        
        # Crear el directorio si no existe
        output_dir = './Data/Fact_tables'
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Directory '{output_dir}' created or already exists.")
        
        # Guardar las dimensiones y la tabla de hechos en archivos CSV
        dimension_vehiculo.to_csv(os.path.join(output_dir, 'dimension_vehiculo.csv'), index=False)
        dimension_vendedor.to_csv(os.path.join(output_dir, 'dimension_vendedor.csv'), index=False)
        dimension_ratings.to_csv(os.path.join(output_dir, 'dimension_ratings.csv'), index=False)
        tabla_hechos.to_csv(os.path.join(output_dir, 'fact_table.csv'), index=False)
        area_dim.to_csv(os.path.join(output_dir, 'area_dim.csv'), index=False)
        product_dim.to_csv(os.path.join(output_dir, 'product_dim.csv'), index=False)
        details_dim.to_csv(os.path.join(output_dir, 'details_dim.csv'), index=False)
        fuel_fact.to_csv(os.path.join(output_dir, 'fuel_fact.csv'), index=False)
        
        logging.info("CSV files saved successfully.")
        
        # Devolver todos los resultados procesados como JSON
        return {
            'dimension_vehiculo': dimension_vehiculo.to_json(orient='records'),
            'dimension_vendedor': dimension_vendedor.to_json(orient='records'),
            'dimension_ratings': dimension_ratings.to_json(orient='records'),
            'fact_table': tabla_hechos.to_json(orient='records'),
            'area_dim': area_dim.to_json(orient='records'),
            'product_dim': product_dim.to_json(orient='records'),
            'details_dim': details_dim.to_json(orient='records'),
            'fuel_fact': fuel_fact.to_json(orient='records')
        }

    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        return None


def clean_api_data(**kwargs):
    try:
        ti = kwargs['ti']
        json_data = json.loads(ti.xcom_pull(task_ids="extract_API"))
        df = pd.json_normalize(data=json_data)
        
        #Drop columns
        drop_columns = ['duoarea', 'units', 'series']  
        df = df.drop(columns=drop_columns)

        #Rename columns (value)
        df = df.rename(columns={'value': 'value($/GAL)'})

        ############################################### Formats

        #Correct the types:
        df['period'] = pd.to_datetime(df['period'], format='%Y-%m-%d') #Object to date
        df = df.astype({col: 'string' for col in df.select_dtypes(include='object').columns}) #Object to string


        ############################################### Cleaning / Replace values
        replaces = {
            'PADD 5 EXCEPT CALIFORNIA': 'West Coast (except California)','PADD 4': 'Rocky Mountain',
            'PADD 2': 'Midwest','PADD 5': 'West Coast','PADD 3': 'Gulf Coast','PADD 1C': 'East Coast (Central)',
            'PADD 1B': 'East Coast (North)','PADD 1A': 'East Coast (South)','PADD 1': 'East Coast'
        } #Create a dictionary to replace the PADD values to a more explicit name
        df['area-name'] = df['area-name'].replace(replaces) #Replaces



        #Make a list for the codes to know if an area is city/state/region
        city_list = ['DENVER', 'NEW YORK CITY', 'SAN FRANCISCO', 'MIAMI', 'CLEVELAND', 
                    'CHICAGO', 'SEATTLE', 'HOUSTON', 'LOS ANGELES', 'BOSTON']

        state_list = ['TEXAS', 'NEW YORK', 'COLORADO', 'CALIFORNIA', 'MINNESOTA', 'FLORIDA', 'MASSACHUSETTS', 
                    'WASHINGTON', 'OHIO']

        region_list = ['West Coast (except California)', 'Rocky Mountain', 'Midwest', 'West Coast', 
                    'Gulf Coast', 'East Coast (Central)', 'East Coast (North)', 'U.S.', 
                    'East Coast (South)', 'East Coast']

        #Create the column 'area' based in 'area-name' values (If they are reffering to a city/state/region)
        df['area'] = np.where(df['area-name'].isin(city_list), 'City',
                    np.where(df['area-name'].isin(state_list), 'State', 
                    np.where(df['area-name'].isin(region_list), 'Region', df['area-name'])))




        #Make a list for the codes to gasoline/diesel
        gasoline_codes = ['EPM0', 'EPMM', 'EPMP', 'EPMR', 'EPMMR', 'EPMRR', 'EPM0R', 'EPMMU', 'EPMPR','EPMPU', 'EPM0U', 'EPMRU']
        diesel_codes = ['EPD2DXL0', 'EPD2D']

        #Replaces
        df['product'] = np.where(df['product'].isin(gasoline_codes), 'Gasoline', 
                        np.where(df['product'].isin(diesel_codes), 'Diesel', df['product']))


        ############################################### Nulls cleaning


        df.dropna(subset=['value($/GAL)'], inplace=True) #No nulls


        ############################################### Save
        logging.info("Data cleaned successfully")

    
        return df.to_json(orient='records')
        
    
    except Exception as e:
        logging.error(f"Error cleaning API data: {str(e)}")
        return None