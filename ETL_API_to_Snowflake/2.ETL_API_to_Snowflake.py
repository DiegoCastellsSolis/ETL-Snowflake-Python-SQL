import snowflake.connector
from botocore.exceptions import NoCredentialsError
import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import requests
import pandas as pd
from datetime import datetime, timedelta


# Configuración de Snowflake
snowflake_account = 'qab35634.us-east-1'  
snowflake_user = 'DIEGOPILA99'
snowflake_password = '$Conditions2023'
snowflake_database = 'Ephifany'
snowflake_schema = 'public'
snowflake_table = 'dolar'
snowflake_warehouse = 'COMPUTE_WH'

 
def create_DB(engine):
    
    # Nombre del archivo SQL que deseas abrir   
    nombre_archivo = "./DDL.txt"

    with open(nombre_archivo, 'r') as archivo_sql:
        contenido = archivo_sql.read()
        
        try:
            print('Se ejecutaran las consultas:')
            for instr in contenido.split('\n'):
                print(instr)
                engine.execute(instr)
            print("Consultas SQL ejecutadas con éxito.") 
            print("") 
        except:
            print('Error al ejecutar las consultas SQL')
 
def extract(url): 
   
    # Realizar la solicitud GET a la API
    response = requests.get(url)
    
    # Convertir los datos JSON a un diccionario
    if response.status_code == 200:
        data = response.json()
    else:
        data = {}
    return data


def  transform(data):
     
    # Crear listas para los datos
    dates = []
    sources = []
    blue_buy = []
    blue_sell = []

    # Extraer los datos necesarios del diccionario
    for entry in data:
        #print(entry)
        date = entry["date"]
        source = entry["source"]  
        buy = entry["value_buy"]
        sell = entry["value_sell"]

        dates.append(date)
        sources.append(source)
        blue_buy.append(buy)
        blue_sell.append(sell)

    # Crear un DataFrame con los datos|
    df = pd.DataFrame({
        "Date": dates,
        "Sources":sources,
        "Blue Buy": blue_buy,
        "Blue Sell": blue_sell
    })

    # Pivotear los datos para crear las columnas de 'Oficial Buy' y 'Oficial Sell'
    pivot_df = df.pivot(index='Date', columns='Sources', values=['Blue Buy', 'Blue Sell'])

    # Renombrar las columnas generadas
    pivot_df.columns = [' '.join(col).strip() for col in pivot_df.columns.values]

    # Resetear el índice
    pivot_df.reset_index(inplace=True)

    # Renombrar las columnas
    nuevos_nombres = {
        'Date' : 'date_operation',
        'Blue Buy Blue': 'blue_buy',
        'Blue Buy Oficial': 'oficial_buy',
        'Blue Sell Blue': 'blue_sell',
        'Blue Sell Oficial': 'oficial_sell'
    }

    pivot_df.rename(columns=nuevos_nombres, inplace=True)
    pivot_df['country'] = 'Argentina'

    return pivot_df


def load(df):
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
        engine = create_engine(connection_url)

        create_DB(engine)

        # Supongamos que 'df' es tu DataFrame
        df.to_sql(name='dolar', con=engine, if_exists='replace', index=False)
        
        print(f"Las cotizaciones argentinas fueron cargadas con éxito en Snowflake.")
    except Exception as e:
        print(f"Error al cargar las cotizaciones argentinas en Snowflake: {str(e)}")
        
def ETL(url):
    
    try:      
            
        data = extract(url)

        if data:
            # Realizar operación si la data no es nula
            df = transform(data)
            load(df)
        else:
            # Realizar alguna otra acción si la data es nula
            # Por ejemplo, mostrar un mensaje de error
            print("La data está vacía o nula.")
    
    except NoCredentialsError:
        print("No se encontraron credenciales de AWS válidas.")
        

    
def main():
    
    # URL de la API para obtener datos históricos en formato JSON
    url = "https://api.bluelytics.com.ar/v2/evolution.json"
    try:
        ETL(url) 
    except NoCredentialsError:
        print("No se encontraron credenciales de Snowflake válidas.")
 

if __name__ == "__main__":
    main()
