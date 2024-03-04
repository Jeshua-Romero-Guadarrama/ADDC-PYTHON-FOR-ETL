from Scripts.Conexion import get_connection, close_connection
from pathlib import Path
import polars as pl

def ImportDataFromExcel(filepath: str, sheet: str = 'Hoja1') -> pl.DataFrame:
    """
        Este método recibe como entrada la ruta del libro de excel atenciones y la hoja de la cual extraerá los datos.
        Recuerda que se deberá indicar el tipo de dato fecha para las columnas: Fecha Creacion y Fecha Cierre.

        Este método deberá validar que la variable filepath es un archivo, caso contrario retornará None.

        El metodo retorna un dataframe con la data importada del archivo especificado.
    """
    # Validar que el filepath corresponde a un archivo
    if not Path(filepath).is_file():
        print("El archivo especificado no existe.")
        return None
    
    try:
        # Leer el archivo Excel y especificar el tipo de dato para las fechas
        df = pl.read_excel(filepath, sheet_name = sheet)
        # Convertir las columnas de fecha, asumiendo formato "YYYY-MM-DD"
        df = df.with_columns([
            pl.col("Fecha Creacion").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("Fecha Cierre").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        ])
        return df
    except Exception as e:
        print(f"Error al importar datos de Excel: {e}")
        return None

def ImportDataFromPostgreSQL(tabla: str, columnas: list) -> pl.DataFrame:
    """
        Este método permita extraer datos de la base datos Atenciones.

        Los parámetros indican cual es la tabla y el nombre de las columnas que se necesitan extraer.

        Recuerde que aquí se debe abrir una conexión para realizar la consulta de tipo select y al finalizar deberá cerrar la conexión.

        El método retornará un dataframe con la data extraída de la base de datos.

        Para concatenar los valores de la lista Columnas use el siguiente código: ', '.join(columnas)
    """
    conn = get_connection()
    if conn is None:
        print("No se pudo establecer conexión con la base de datos.")
        return None
    
    try:
        cursor = conn.cursor()
        
        # Asegurarse de que los nombres de las columnas y la tabla estén entre comillas dobles
        columnas_citadas = [f'"{col}"' for col in columnas]
        tabla_citada = f'"{tabla}"'
        
        # Crear la consulta SQL con identificadores entre comillas dobles
        sql_query = f'SELECT {", ".join(columnas_citadas)} FROM {tabla_citada}'
        
        cursor.execute(sql_query)
        
        # Obtener los datos y convertirlos a un DataFrame de Polars
        column_names = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        df = pl.DataFrame(data, schema=column_names)
        return df
    except Exception as e:
        print(f"Error al importar datos de PostgreSQL: {e}")
        return None
    finally:
        if conn:
            conn.close()
            