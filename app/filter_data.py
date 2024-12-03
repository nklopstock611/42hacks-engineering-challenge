import pandas

def filter_airports(dataframe, column_name='wikipedia_link'):
    """
    Filtra las filas del dataset "dataframe" que no tengan valor en la columna "column_name".
    En este caso particular, se filtran los aeropuertos que no tienen link a Wikipedia.
    
    Args:
        dataframe (DataFrame): El dataset a filtrar.
        column_name (str): El nombre de la columna a filtrar.
        
    Returns:
        DataFrame: El dataset filtrado.
    """
    filtered_df = dataframe[dataframe[column_name].notna() & (dataframe[column_name] != '')]
    
    return filtered_df

if __name__ == '__main__':
    df = pandas.read_csv('data/airports.csv')
    df = filter_airports(df)
    df.to_csv('data/airports_w_wiki.csv', index=False)
