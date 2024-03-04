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
        # Convertir columnas de fecha desde la importación (después de la carga)
        df = df.with_columns([
            pl.col("Fecha Creacion").str.strptime(pl.Date, "%d-%m-%Y", strict = False),
            pl.col("Fecha Cierre").str.strptime(pl.Date, "%d-%m-%Y", strict = False)
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
        # Crear la consulta SQL
        sql_query = f"SELECT {', '.join(columnas)} FROM {tabla}"
        # Ejecutar la consulta y cargar los datos en un DataFrame de Polars
        df = pl.read_sql(sql_query, conn)
        return df
    except Exception as e:
        print(f"Error al importar datos de PostgreSQL: {e}")
        return None
    finally:
        close_connection(conn)