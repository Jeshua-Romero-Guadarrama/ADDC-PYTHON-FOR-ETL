from dotenv import load_dotenv
import psycopg2
import os 

# Cargar las variables de entorno desde el archivo '.env':
load_dotenv(".env")

# Recuperar las variables de entorno:
host     = os.getenv('HOST')
dbname   = os.getenv('DB_NAME')
user     = os.getenv('USER')
password = os.getenv('PASSWORD')

def get_connection():

    """
        Este método crea una conexión con la base de datos 'Atenciones' y retorna la conexión creada.
    """
    try:
        # Establecer la conexión utilizando los datos cargados de las variables de entorno
        conn = psycopg2.connect(
            host    = host,
            dbname  = dbname,
            user    = user,
            password= password
        )
        # Establecer la conexión
        return conn
    except Exception as e:
        print(f"Ocurrió un error al conectar a la base de datos: {e}")
        return None

def close_connection(conn: psycopg2):
    """
        Este método recibe como parámetro una conexión y lo que hace es cerrar esa conexión. 
        No retorna nada como resultado
    """
    try:
        # Verificar si la conexión está abierta antes de intentar cerrarla
        if conn is not None and not conn.closed:
            # Cerrar la conexión
            conn.close()
    except Exception as e:
        print(f"Ocurrió un error al cerrar la conexión: {e}")