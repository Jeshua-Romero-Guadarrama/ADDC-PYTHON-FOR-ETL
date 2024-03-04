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
    # Selecciona las columnas especificadas.
    selected_data = data.select(list(args))
    
    # Elimina duplicados basándose en la primera columna especificada.
    transformed_data = selected_data.unique(subset=[args[0]])
    
    return transformed_data

def CreateFactTable(Atenciones: pl.DataFrame, Proveedor: pl.DataFrame, Estado: pl.DataFrame, TipoAtencion: pl.DataFrame) -> pl.DataFrame:
    
    """
    Recibe como parámetro los datframes que se combinarán, de este dataframe combinado se seleccionan
    solo las columnas que se usarán para cargar a la tabla tala de hechos.

    El método retorna el dataframe consolidado.
    """
    try:
        # Los joins se deben ajustar para usar los nombres de columnas correctos. Aquí se asume que Atenciones tiene columnas que se refieren a IDs o nombres que pueden mapearse a Proveedor, Estado, y TipoAtencion
        df = (Atenciones
              .join(Proveedor, on='Proveedor', how='left')  # Asegúrate de que 'ProveedorID' sea el nombre correcto
              .join(Estado, on='Estado', how='left')        # Asegúrate de que 'EstadoID' sea el nombre correcto
              .join(TipoAtencion, on='TipoAtencion', how='left')  # Asegúrate de que 'TipoAtencionID' sea el nombre correcto
             )
        
        # Seleccionar las columnas necesarias para la tabla de hechos
        fact_table = df.select([
            'TicketID', 'EstadoID', "TipoAtencionID", "FechaCreacion", "FechaCierre", "AgenciaID", "ItemID", "ProveedorID"
            # Añade o quita columnas según sea necesario
        ])
        
        return fact_table
    except Exception as e:
        print(f"Error al crear la tabla de hechos: {e}")
        # Retorna un dataframe vacío si ocurre un error
        return pl.DataFrame()