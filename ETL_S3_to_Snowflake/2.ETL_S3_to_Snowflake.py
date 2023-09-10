import snowflake.connector
import boto3
from botocore.exceptions import NoCredentialsError
import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL


# Configuración de Snowflake
snowflake_account = 'qab35634.us-east-1'  
snowflake_user = 'DIEGOPILA99'
snowflake_password = '$Conditions2023'
snowflake_database = 'Ephifany'
snowflake_schema = 'public'
snowflake_table = 'ventas'
snowflake_warehouse = 'COMPUTE_WH'

# Configuración de AWS S3
aws_access_key_id = 'AKIAR5RS6YJC4JUMFHVN'
aws_secret_access_key = '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'
s3_bucket = 'ventas2019'
 
 
def extract(s3,s3_object_key): 
    # Leer el archivo CSV desde S3
    csv_obj = s3.get_object(Bucket=s3_bucket, Key=s3_object_key)
    
    # Cargar el contenido del archivo CSV en un DataFrame de pandas
    df = pd.read_csv(csv_obj['Body'])

    return df

# Función para corregir años de dos dígitos
def fix_two_digit_years(date_str):
    parts = date_str.split(' ')
    if len(parts) == 2:
        date_part, time_part = parts
        date_parts = date_part.split('/')
        if len(date_parts) == 3:
            month, day, year = date_parts
            if len(year) == 2:
                year = '20' + year  # Agregar "20" a años de dos dígitos
            return f"{month}/{day}/{year} {time_part}"
    return date_str

def  transform(df,s3_object_key):
    # Mapping
    column_mapping = {
        'Order ID': 'order_id',
        'Product': 'product',
        'Quantity Ordered': 'quantity_ordered',
        'Price Each': 'price_each',
        'Order Date': 'order_date',
        'Purchase Address': 'purchase_address'
    }      

    df = df.rename(columns=column_mapping) 
    # Eliminar las filas donde 'order_id' sea nulo
    df = df.dropna(subset=['order_id'])

    # Verifica el formato de fecha y hora antes de la conversión
    valid_format = df['order_date'].str.match(r'\d{2}/\d{2}/\d{2} \d{2}:\d{2}')

    # Aplica la corrección solo a las filas con formato válido
    df.loc[valid_format, 'order_date'] = df.loc[valid_format, 'order_date'].apply(
        lambda x: pd.to_datetime(x, format='%m/%d/%y %H:%M').strftime('%m/%d/%Y %H:%M')
    )
    print(f'el archivo {s3_object_key} fue procesado correctamente')

    return df

def load(df,s3_object_key):
    try:

        # Configura la cadena de conexión de Snowflake
        connection_url = URL(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse
        )

        # Crea una conexión SQLAlchemy para pandas
        #engine = create_engine(f'snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}')
        engine = create_engine(connection_url)


        # Supongamos que 'df' es tu DataFrame
        df.to_sql(name='ventas', con=engine, if_exists='replace', index=False)

        # Cierra la conexión
        
        print(f"Archivo {s3_object_key} cargado con éxito en Snowflake.")
    except Exception as e:
        print(f"Error al cargar el archivo {s3_object_key} en Snowflake: {str(e)}")
        
def ETL():
    
    try:
        # Conexión a AWS S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        print('')
        # Lista de objetos en el bucket de S3
        s3_objects = s3.list_objects(Bucket=s3_bucket)
        for s3_object in s3_objects.get('Contents', []):
            s3_object_key = s3_object['Key']
            
            df = extract(s3,s3_object_key)

            df = transform(df,s3_object_key)
           
            load(df,s3_object_key)

            print('')
            print('')

        return 'ok'
    except NoCredentialsError:
        print("No se encontraron credenciales de AWS válidas.")
        return 'ups'

    
def main():
    # Conexión a Snowflake
    try:
        ETL() 
    except NoCredentialsError:
        print("No se encontraron credenciales de Snowflake válidas.")
 

if __name__ == "__main__":
    main()
