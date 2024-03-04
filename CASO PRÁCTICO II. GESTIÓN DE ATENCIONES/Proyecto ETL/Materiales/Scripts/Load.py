from Scripts.Conexion import get_connection, close_connection
import polars as pl
import io


def InsertDimToPostgreSQL(data: pl.DataFrame, nombreTabla: str) -> None:

    """
    Este método le permitirá cargar los datos hacia las tablas de dimensiones.

    Recibe como parámetro el dataframe con la data que se cargará y la tabla sobre
    la cual se cargarán los datos.

    Recuerda que este método debe permitir cargar los datos sobre columnas específicas, es decir,
    puede que una tabla tenga 05 columnas, pero el dataframe solo tenga 04 columnas, entonces
    se deberá cargar cada columna del dataframe sobre su correspondiente columna en la tabla de postgresql

    Revise el video la clase para más ayuda.
    
    """
    conn = get_connection()
    if conn is None:
        print("No se pudo establecer la conexión con la base de datos.")
        return
    cursor = conn.cursor()
    
    # Convertir el DataFrame de Polars a CSV en memoria
    buffer = io.StringIO()
    data.write_csv(buffer, has_header=False)
    buffer.seek(0)  # Regresar al inicio del buffer para leer los datos
    
    # Preparar la consulta COPY para cargar los datos
    # Las columnas del DataFrame deben coincidir con el orden y nombre de las columnas en la tabla destino
    column_names = ', '.join(data.columns)
    copy_query = f"COPY {nombreTabla}({column_names}) FROM STDIN WITH (FORMAT csv)"
    
    try:
        # Ejecutar la consulta COPY
        cursor.copy_expert(copy_query, buffer)
        conn.commit()
    except Exception as e:
        print(f"Error al insertar datos en {nombreTabla}: {e}")
        conn.rollback()
    finally:
        buffer.close()
        cursor.close()
        close_connection(conn)


def InsertFactToPostgreSQL(data: pl.DataFrame, nombreTabla: str) -> None:

    """
    Este método le permitirá cargar los datos hacia la tabla de hechos FactAtenciones.

    Recibe como parámetro el dataframe con la data que se cargará y la tabla sobre
    la cual se cargarán los datos.

    Como el Dataframe de entrada cuenta con todas las columnas que se cargarán,
    no se requiere que se especifiqen las columnas durante la carga de datos

    Revise el video la clase para más ayuda.
    
    """
    conn = get_connection()
    if conn is None:
        print("No se pudo establecer la conexión con la base de datos.")
        return
    cursor = conn.cursor()
    
    # Convertir el DataFrame de Polars a CSV en memoria
    buffer = io.StringIO()
    data.write_csv(buffer, has_header=False)
    buffer.seek(0)  # Regresar al inicio del buffer para leer los datos
    
    # Preparar la consulta COPY sin especificar columnas, ya que el DataFrame tiene todas las necesarias
    copy_query = f"COPY {nombreTabla} FROM STDIN WITH (FORMAT csv)"
    
    try:
        # Ejecutar la consulta COPY
        cursor.copy_expert(copy_query, buffer)
        conn.commit()
    except Exception as e:
        print(f"Error al insertar datos en {nombreTabla}: {e}")
        conn.rollback()
    finally:
        buffer.close()
        cursor.close()
        close_connection(conn)