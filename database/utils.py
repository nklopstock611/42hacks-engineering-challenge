from sqlalchemy.sql import text
from sqlalchemy.orm import Session

from database.models import NearestAirport

"""
with Session(engine) as session:
"""

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

def create_user_id_index(session):
    """
    Crea un índice en la columna user_id de la tabla nearest_airport.
    """
    try:
        query = text("""
            CREATE INDEX IF NOT EXISTS idx_user_id ON nearest_airport (user_id);
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
