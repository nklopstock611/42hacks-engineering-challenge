import os
import csv
from sqlalchemy.sql import text
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from database.models import NearestAirport

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

def insert_bulk_data(session: Session, data_batch: list, temp_file="temp_data.csv"):
    """
    Inserta un lote de datos en la tabla NearestAirport utilizando el comando COPY de PostgreSQL.

    :param session: La sesión activa de la base de datos.
    :param data_batch: Una lista de diccionarios con los datos a insertar.
                       Cada diccionario debe tener las claves `user_id` y `airport_id`.
    :param temp_file: Nombre del archivo temporal para la carga de datos.
    """
    if not data_batch:
        print("No hay datos para insertar.")
        return

    # Escribir los datos en un archivo CSV temporal
    try:
        with open(temp_file, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["user_id", "airport_id"])  # Escribe los encabezados
            for record in data_batch:
                writer.writerow([record["user_id"], record["airport_id"]])

        print(f"Archivo temporal creado con {len(data_batch)} registros.")

        # Usar el comando COPY para insertar los datos desde el archivo
        # Este comando hace más eficiente la inserción de grandes volúmenes de datos
        connection = session.connection().connection
        with open(temp_file, mode='r', encoding='utf-8') as csvfile:
            cursor = connection.cursor()
            cursor.copy_expert(
                """
                COPY nearestairport (user_id, airport_id)
                FROM STDIN
                WITH CSV HEADER
                """,
                csvfile
            )
        session.commit()
        print(f"Insertados {len(data_batch)} registros en la tabla NearestAirport.")
    except Exception as e:
        session.rollback()
        print(f"Error durante la inserción masiva: {e}")
    finally:
        # Elimina el archivo temporal
        if os.path.exists(temp_file):
            os.remove(temp_file)

def create_user_id_index(session):
    """
    Crea un índice en la columna user_id de la tabla nearest_airport.
    """
    try:
        query = text("""
            CREATE INDEX IF NOT EXISTS idx_user_id ON nearestairport (user_id);
        """)
        session.execute(query)
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
        FROM nearest_airport 
        WHERE user_id = :user_id
    """)
    result = session.execute(query, { "user_id": user_id }).fetchone()
    return result
