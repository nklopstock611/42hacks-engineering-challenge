import os
import csv
import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy.orm import Session

from database.models import AirportWikiLink, NearestAirport

def insert_data(session: Session, user_id: int, airport_id: int):
    """
    Inserta un nuevo registro en la tabla de usuarios - aeropuerto.
    """
    pair = NearestAirport(
        user_id=user_id,
        airport_id=airport_id
    )
    try:     
        session.add(pair)
        session.commit()
        session.refresh(pair)
        print("Insert successful!")
    except Exception as e:
        print(f"Error: {e}")

def insert_bulk_data(session: Session, data_batch: list, table: str, col1: str, col2: str, temp_file="temp_data.csv"):
    """
    Inserta un lote de datos en la tabla NearestAirport utilizando el comando COPY de PostgreSQL.
    """
    if not data_batch:
        print("No hay datos para insertar.")
        return

    # Escribir los datos en un archivo CSV temporal
    try:
        with open(temp_file, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([col1, col2])  # Escribe los encabezados
            for record in data_batch:
                writer.writerow([record[col1], record[col2]])

        print(f"Archivo temporal creado con {len(data_batch)} registros.")

        # Usar el comando COPY para insertar los datos desde el archivo
        # Este comando hace más eficiente la inserción de grandes volúmenes de datos
        connection = session.connection().connection
        with open(temp_file, mode='r', encoding='utf-8') as csvfile:
            cursor = connection.cursor()
            query = f"""
                COPY {table} ({col1}, {col2})
                FROM STDIN
                WITH CSV HEADER            
            """
            cursor.copy_expert(query, csvfile)
        session.commit()
        print(f"Insertados {len(data_batch)} registros en la tabla NearestAirport.")
    except Exception as e:
        session.rollback()
        print(f"Error durante la inserción masiva: {e}")
    finally:
        # Elimina el archivo temporal
        if os.path.exists(temp_file):
            os.remove(temp_file)

def get_wiki_data(airports_file="data/airports_w_wiki.csv"):
    """
    Retorna un arreglo de diccionarios con los datos de aeropuertos y sus enlaces a Wikipedia.
    """
    airports = pd.read_csv(airports_file)
    data = [{"airport_id": row["id"], "wikipedia_link": row["wikipedia_link"]} for _, row in airports.iterrows()]
    
    return data

def create_index(session, table, column):
    """
    Crea un índice en la columna user_id de la tabla nearest_airport.
    """
    index_name = f"idx_{column}"
    try:
        query = f"""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column});
        """
        session.execute(text(query))
        session.commit()
        print("Índice creado exitosamente")
    except Exception as e:
        print(f"Error al crear el índice: {e}")
        session.rollback()

def get_user_airport(session, user_id):
    """
    Query para obtener el aeropuerto más cercano a un usuario.
    """
    query = text("""
        SELECT airport_id 
        FROM nearestairport 
        WHERE user_id = :user_id
    """)
    result = session.execute(query, { "user_id": user_id }).fetchone()
    return result[0] if result else None

def get_airports_wiki_link(session, airport_id):
    """
    Query para obtener el enlace a Wikipedia de un aeropuerto.
    """
    query = text("""
        SELECT wikipedia_link 
        FROM airportwikilink
        WHERE airport_id = :airport_id
    """)
    result = session.execute(query, { "airport_id": airport_id }).fetchone()
    return result[0] if result else None
