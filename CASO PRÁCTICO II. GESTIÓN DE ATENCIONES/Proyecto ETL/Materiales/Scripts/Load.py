from Scripts.Conexion import get_connection, close_connection
import polars as pl
import psycopg2
import io

def CrearTablas():
    # Asume que get_connection() es una función previamente definida que establece la conexión con tu base de datos PostgreSQL
    conn = get_connection()
    if conn is None:
        print("No se pudo establecer la conexión con la base de datos.")
        return

    try:
        cursor = conn.cursor()

        # Definir las instrucciones DROP TABLE para eliminar las tablas existentes
        drop_queries = [
            'DROP TABLE IF EXISTS "factAtenciones" CASCADE;',
            'DROP TABLE IF EXISTS "dimAgencia" CASCADE;',
            'DROP TABLE IF EXISTS "dimItem" CASCADE;',
            'DROP TABLE IF EXISTS "dimProveedor" CASCADE;',
            'DROP TABLE IF EXISTS "dimEstado" CASCADE;',
            'DROP TABLE IF EXISTS "dimTipoAtencion" CASCADE;'
        ]

        # Ejecutar instrucciones DROP TABLE
        for query in drop_queries:
            cursor.execute(query)
            print("Tabla eliminada correctamente.")

        # Definir las consultas CREATE TABLE para cada tabla de forma individual
        create_queries = [
            """
            CREATE TABLE IF NOT EXISTS "dimAgencia" (
                "AgenciaID" SERIAL PRIMARY KEY,
                "Agencia" VARCHAR(75),
                "Direccion" VARCHAR(255),
                "Distrito" VARCHAR(50),
                "Provincia" VARCHAR(50),
                "Departamento" VARCHAR(30)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "dimItem" (
                "ItemID" SERIAL PRIMARY KEY,
                "Item" VARCHAR(75),
                "TipoItem" VARCHAR(255),
                "Categoria" VARCHAR(50)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "dimProveedor" (
                "ProveedorID" SERIAL PRIMARY KEY,
                "Proveedor" VARCHAR(75),
                "TipoProveedor" VARCHAR(20)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "dimEstado" (
                "EstadoID" SERIAL PRIMARY KEY,
                "Estado" VARCHAR(20)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "dimTipoAtencion" (
                "TipoAtencionID" SERIAL PRIMARY KEY,
                "TipoAtencion" VARCHAR(20)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS "factAtenciones" (
                "TicketID" VARCHAR(50),
                "EstadoID" INT,
                "TipoAtencionID" INT,
                "FechaCreacion" DATE,
                "FechaCierre" DATE,
                "AgenciaID" INT,
                "ItemID" INT,
                "ProveedorID" INT,
                CONSTRAINT "fk_estadoID" FOREIGN KEY ("EstadoID") REFERENCES "dimEstado"("EstadoID"),
                CONSTRAINT "fk_tipoAtencionID" FOREIGN KEY ("TipoAtencionID") REFERENCES "dimTipoAtencion"("TipoAtencionID"),
                CONSTRAINT "fk_proveedorID" FOREIGN KEY ("ProveedorID") REFERENCES "dimProveedor"("ProveedorID"),
                CONSTRAINT "fk_agenciaID" FOREIGN KEY ("AgenciaID") REFERENCES "dimAgencia"("AgenciaID"),
                CONSTRAINT "fk_itemID" FOREIGN KEY ("ItemID") REFERENCES "dimItem"("ItemID")
            );
            """
        ]

        # Ejecutar instrucciones CREATE TABLE, una por una
        for query in create_queries:
            cursor.execute(query)
            print("Tabla creada correctamente.")

        # Confirmar los cambios
        conn.commit()
        print("Todas las tablas han sido creadas exitosamente.")
    except Exception as e:  # Captura un tipo de excepción más genérico si no estás seguro de que sea psycopg2.Error
        print(f"Error al ejecutar las consultas: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

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
        
 
    try:
        # Obtener nombres de columnas del DataFrame
        columnas_df = list(data.columns)
        
        # Crear la lista de columnas para la sentencia SQL, asegurándose de que los nombres de columnas estén entre comillas dobles
        columnas_sql = ', '.join([f'"{col}"' for col in columnas_df])
        
        # Crear la lista de placeholders para la sentencia SQL
        placeholders = ', '.join(['%s' for _ in columnas_df])
        
        # Preparar sentencia SQL para insertar datos
        sql = f'INSERT INTO "{nombreTabla}" ({columnas_sql}) VALUES ({placeholders})'
        
        # Convertir DataFrame a una lista de tuplas. Ajusta esta parte según la biblioteca de manejo de datos que estés utilizando.
        if hasattr(data, 'to_pandas'):  # Si es convertible a Pandas DataFrame, como en Polars
            datos = [tuple(row) for row in data.to_pandas().itertuples(index=False)]
        else:
            # Si la biblioteca no es Polars o Pandas, necesitarás adaptar esta línea
            datos = [tuple(row) for row in data.collect()]  # Ajuste hipotético para otra biblioteca
        
        # Ejecutar sentencia SQL para cada fila del DataFrame
        cursor = conn.cursor()
        cursor.executemany(sql, datos)
        conn.commit()
        print("Datos insertados correctamente.")
        
    except Exception as e:
        print(f"Error al insertar los datos: {e}")
    finally:
        if conn is not None:
            conn.close()
            
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

    try:
        cursor = conn.cursor()

        # Crear la lista de columnas para la sentencia SQL, asegurándose de que los nombres de columnas estén entre comillas dobles
        columnas_sql = ', '.join([f'"{col}"' for col in data.columns])

        # Preparar la consulta SQL para insertar datos
        query = f"""INSERT INTO "{nombreTabla}" ({columnas_sql}) VALUES ({", ".join(["%s" for _ in data.columns])})"""

        # Convertir el DataFrame a una lista de tuplas
        datos = [tuple(row) for row in data.to_numpy()]

        # Ejecutar la consulta SQL para cada fila del DataFrame
        cursor.executemany(query, datos)

        # Confirmar la transacción
        conn.commit()

        print("Datos insertados correctamente.")
    
    except Exception as e:
        print(f"Error al insertar los datos: {e}")

    finally:
        if conn is not None:
            # Cerrar el cursor y la conexión
            cursor.close()
            conn.close()