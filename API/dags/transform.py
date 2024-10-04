import pandas as pd
import logging
import os
import json

logging.basicConfig(level=logging.INFO)

def dimensions_vehiculo(**kwargs):
    try:
        ti = kwargs['ti']
        
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
                
        logging.info("Data loaded successfully")
        
        dimension_vehiculo = df[['Year', 'Make', 'Model', 'Drivetrain', 'MinMPG', 'MaxMPG', 
                         'FuelType', 'Transmission', 'Engine']].drop_duplicates().reset_index(drop=True)

        dimension_vehiculo['ID_Vehiculo'] = dimension_vehiculo.index + 1
        
        logging.info(dimension_vehiculo.info())
        
        return dimension_vehiculo.to_json(orient='records')
        
        
    except Exception as e:
        logging.error(f"Error creating dimension table : {str(e)}")
        
        
        
def dimension_vendedor(**kwargs):
    try:
        ti = kwargs['ti']
        
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
                
        logging.info("Data loaded successfully")
        
        dimension_vendedor = df[['SellerName', 'SellerType', 'State', 'Zipcode', 'StreetName']].drop_duplicates().reset_index(drop=True)

        dimension_vendedor['ID_Seller'] = dimension_vendedor.index + 1
        
        logging.info(dimension_vendedor.info())
        
        return dimension_vendedor.to_json(orient='records')
        
        
    except Exception as e:
        logging.error(f"Error creating dimension table : {str(e)}")
        
        

def dimension_ratings(**kwargs):
    try:
        ti = kwargs['ti']
        
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
        
        logging.info("Data loaded successfully")
        
        dimension_ratings = df[['ConsumerRating', 'SellerRating', 'ComfortRating', 'InteriorDesignRating', 
                        'PerformanceRating', 'ValueForMoneyRating', 'ExteriorStylingRating', 
                        'ReliabilityRating', 'DealType']].drop_duplicates().reset_index(drop=True)

        dimension_ratings['ID_Rating'] = dimension_ratings.index + 1
        
        logging.info(dimension_ratings.info())
        
        return dimension_ratings.to_json(orient='records')
        
        
    except Exception as e:
        logging.error(f"Error creating dimension table : {str(e)}")
        
        
        
def fact_table(**kwargs):
    try:
        ti = kwargs['ti']
        
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
        
        json_data = json.loads(ti.xcom_pull(task_ids="dimension_vehiculo"))
        dimension_vehiculo = pd.json_normalize(data=json_data)
        
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
        return tabla_hechos.to_json(orient='records')
        
    except Exception as e:
        logging.error(f"Error creating fact table : {str(e)}")


    
##############################################################################################################################################
##############################################################################################################################################
    
def area_dim(**kwargs):
    try:
        ti = kwargs['ti']
        api_json = json.loads(ti.xcom_pull(task_ids="extract_API"))
        apidf = pd.json_normalize(data=api_json)
        
        logging.info("Data loaded successfully")
        
        area_dim = apidf[['area', 'area-name']].drop_duplicates().reset_index(drop=True)

        area_dim['area_ID'] = area_dim.index + 1

        logging.info(area_dim.head())
        return area_dim.to_json(orient='records')
    
    except Exception as e:
        logging.error(f"Error creating dimension_area table : {str(e)}")
        
def product_dim(**kwargs):
    try:
        ti = kwargs['ti']
        api_json = json.loads(ti.xcom_pull(task_ids="extract_API"))
        apidf = pd.json_normalize(data=api_json)
        
        logging.info("Data loaded successfully")
        
        product_dim = apidf[['product', 'product-name']].drop_duplicates().reset_index(drop=True)

        product_dim['product_ID'] = product_dim.index + 1

        logging.info(product_dim.head())
        return product_dim.to_json(orient='records')
    
    except Exception as e:
        logging.error(f"Error creating dimension_product table : {str(e)}")
        
def details_dim(**kwargs):
    try:
        ti = kwargs['ti']
        api_json = json.loads(ti.xcom_pull(task_ids="extract_API"))
        apidf = pd.json_normalize(data=api_json)
        
        logging.info("Data loaded successfully")
        
        details_dim = apidf[['process', 'process-name', 'series-description']].drop_duplicates().reset_index(drop=True)

        details_dim['details_ID'] = details_dim.index + 1

        logging.info(details_dim.head())
        return details_dim.to_json(orient='records')
    
    except Exception as e:
        logging.error(f"Error creating dimension_details table : {str(e)}")
        
def df_fuel_area (**kwargs):
    try:
        ti = kwargs['ti']
        api_json = json.loads(ti.xcom_pull(task_ids="extract_API"))
        apidf = pd.json_normalize(data=api_json)
        
        logging.info("Data loaded successfully")
        
        df_fuel_area = pd.merge(apidf, area_dim, on=['area', 'area-name'], how='left')

        df_fuel_product = pd.merge(apidf, product_dim, on=['product', 'product-name'], how='left')

        df_fuel_details = pd.merge(apidf, details_dim, on=['process', 'process-name', 'series-description'], how='left')

        fuel_fact = df_fuel_area[['period', 'value($/GAL)']].copy()

        fuel_fact['area_ID'] = df_fuel_area['area_ID']
        fuel_fact['product_ID'] = df_fuel_product['product_ID']
        fuel_fact['details_ID'] = df_fuel_details['details_ID']

        fuel_fact['fuel_ID'] = fuel_fact.index + 1

        fuel_fact = fuel_fact[['fuel_ID', 'period', 'area_ID', 'product_ID', 'details_ID', 'value($/GAL)']]
        
        logging.info(fuel_fact.head())
        return fuel_fact.to_json(orient='records')
    
    except Exception as e:
        logging.error(f"Error creating fuel fact table : {str(e)}")
        
        
###########################################################################################################################
###########################################################################################################################


def process_data(**kwargs):
    try:
        ti = kwargs['ti']
        
        # Extraer y normalizar los datos del dataset
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
        
        # Extraer y normalizar los datos de la API
        api_data = json.loads(ti.xcom_pull(task_ids="extract_API"))
        apidf = pd.json_normalize(data=api_data)
        
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


