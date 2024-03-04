import polars as pl 

def RenameColumns(data: pl.DataFrame, mydict: dict) -> pl.DataFrame:

    '''
        Este método tiene como parámetro el dataframe con los datos, y el diccionario de datos que contiene el nombre actual y el nombre por el cual se cambiarán las columnas

        El método retorna el dataframe con el nombre de las columnas cambiadas
    '''
    try:
        # Renombrar las columnas del dataframe según el diccionario proporcionado
        for old_name, new_name in mydict.items():
            data = data.rename({old_name: new_name})
        return data
    except Exception as e:
        print(f"Error al renombrar columnas: {e}")
        # Retorna el dataframe sin cambios si ocurre un error
        return data

def SplitByDelimiter(data: pl.DataFrame, columna: str) -> pl.DataFrame:
    '''
        Este método tiene como parámetro el dataframe con los datos y el nombre de la columna sobre la cual se aplicará la operación necesaria.
        
        La operación consiste en dividir la columna usando como delimitador " - ".

        El método retorna el dataframe transformado.
    '''
    data = data.with_columns([
        # Dividir la columna 'Ubicacion' usando ' - ' como delimitador y limitar a 2 partes
        pl.col(columna)
        .str.split_exact(' - ', 1)
        .struct.rename_fields(['Columna1', 'Columna2'])
        .alias('SplitResult')
    ]).unnest('SplitResult')
    return data

def CreateDimension(data: pl.DataFrame, *args):

    """
    Recibe como entrada un dataframe y las columnas que se desean seleccionar luego aplica la eliminación de duplicados usando como referencia la primera columna de los datos pasados por parámetro.

    Retorna el dataframe transformado  
    
    """
    try:
        # Seleccionar las columnas especificadas
        selected_columns = data.select(list(args))
        # Eliminar duplicados basados en la primera columna especificada
        result = selected_columns.unique(subset=[args[0]])
        return result
    except Exception as e:
        print(f"Error al crear la dimensión: {e}")
        # Retorna el dataframe sin cambios si ocurre un error
        return data  

def CreateFactTable(Atenciones: pl.DataFrame, Proveedor: pl.DataFrame, Estado: pl.DataFrame, TipoAtencion: pl.DataFrame) -> pl.DataFrame:
    
    """
    Recibe como parámetro los datframes que se combinarán, de este dataframe combinado se seleccionan
    solo las columnas que se usarán para cargar a la tabla tala de hechos.

    El método retorna el dataframe consolidado.
    """
    try:
        # Realizar los joins necesarios
        # Ajusta las columnas de join y las columnas a seleccionar según tu esquema de datos
        df = (Atenciones
              .join(Proveedor, on='proveedor_id', how='left')  # Ajustar las columnas de join
              .join(Estado, on='estado_id', how='left')        # Ajustar las columnas de join
              .join(TipoAtencion, on='tipo_atencion_id', how='left')  # Ajustar las columnas de join
             )
        
        # Seleccionar las columnas necesarias para la tabla de hechos
        # Este es un ejemplo genérico, ajusta las columnas según necesites
        fact_table = df.select([
            'atencion_id', 'fecha_atencion', 'proveedor_nombre', 'estado_descripcion', 'tipo_atencion_descripcion'
            # Añade o quita columnas según sea necesario
        ])
        
        return fact_table
    except Exception as e:
        print(f"Error al crear la tabla de hechos: {e}")
        # Retorna un dataframe vacío si ocurre un error
        return pl.DataFrame()