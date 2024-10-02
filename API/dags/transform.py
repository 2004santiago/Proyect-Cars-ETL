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
        
    except Exception as e:
        logging.error(f"Error creating fact table : {str(e)}")
        
###########################################################################################################################
###########################################################################################################################

def process_data(**kwargs):
    try:
        ti = kwargs['ti']
        json_data = json.loads(ti.xcom_pull(task_ids="extract_dataset"))
        df = pd.json_normalize(data=json_data)
                
        logging.info("Data loaded successfully")

        dimension_vehiculo = df[['Year', 'Make', 'Model', 'Drivetrain', 'MinMPG', 'MaxMPG', 
                                'FuelType', 'Transmission', 'Engine']].drop_duplicates().reset_index(drop=True)
        dimension_vehiculo['ID_Vehiculo'] = dimension_vehiculo.index + 1
        logging.info("Dimension Vehiculo created")

        dimension_vendedor = df[['SellerName', 'SellerType', 'State', 'Zipcode', 'StreetName']].drop_duplicates().reset_index(drop=True)
        dimension_vendedor['ID_Seller'] = dimension_vendedor.index + 1
        logging.info("Dimension Vendedor created")

        dimension_ratings = df[['ConsumerRating', 'SellerRating', 'ComfortRating', 'InteriorDesignRating', 
                                'PerformanceRating', 'ValueForMoneyRating', 'ExteriorStylingRating', 
                                'ReliabilityRating', 'DealType']].drop_duplicates().reset_index(drop=True)
        dimension_ratings['ID_Rating'] = dimension_ratings.index + 1
        logging.info("Dimension Ratings created")
        
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
        
        output_dir = './Data/Tablas de hechos'
        
        dimension_vehiculo.to_csv(os.path.join(output_dir, 'dimension_vehiculo.csv'), index=False)
        dimension_vendedor.to_csv(os.path.join(output_dir, 'dimension_vendedor.csv'), index=False)
        dimension_ratings.to_csv(os.path.join(output_dir, 'dimension_ratings.csv'), index=False)
        tabla_hechos.to_csv(os.path.join(output_dir, 'fact_table.csv'), index=False)
        logging.info("CSV files saved successfully.")

        return {
            'dimension_vehiculo': dimension_vehiculo.to_json(orient='records'),
            'dimension_vendedor': dimension_vendedor.to_json(orient='records'),
            'dimension_ratings': dimension_ratings.to_json(orient='records'),
            'tabla_hechos': tabla_hechos.to_json(orient='records')
        }
        
    except Exception as e:
        logging.error(f"Error processing data: {str(e)}")
        return None
