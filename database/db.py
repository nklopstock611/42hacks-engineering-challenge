import time
from sqlmodel import Session, SQLModel, create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

import app.config as config
import app.utils as app_utils
import database.utils as db_utils

API_LIMIT = 60  # Límite de solicitudes por segundo
BATCH_SIZE = 1000  # Tamaño de los lotes para la base de datos
RETRY_LIMIT = 3  # Número de reintentos en caso de error

engine = create_engine(config.DB_URL)

def create_db_and_tables(engine_):
    SQLModel.metadata.create_all(engine_)

def process_user(user_id):
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            airport_id = app_utils.find_nearest_airport(user_id)['airport_id']
            print(f"Usuario {user_id} procesado: Aeropuerto más cercano {airport_id}")
            return {"user_id": user_id, "airport_id": airport_id}
        except Exception as e:
            print(f"Intento {attempt} fallido para usuario {user_id}: {e}")
            time.sleep(1)  # Espera antes de reintentar
    print(f"Usuario {user_id} falló después de {RETRY_LIMIT} intentos.")
    return None

def batch_insert_to_db(data_batch):
    try:
        with Session(engine) as session:
            db_utils.insert_bulk_data(session, data_batch, 'nearestairport', 'user_id', 'airport_id')
        print(f"Lote de {len(data_batch)} usuarios insertado en la base de datos.")
    except Exception as e:
        print(f"Error al insertar el lote en la base de datos: {e}")

if __name__ == "__main__":
    create_db_and_tables(engine)
    print('Base de datos y tablas creadas.')

    # Carga de datos de usuarios

    user_ids = range(1, 100001)
    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=API_LIMIT) as executor:
        futures = [executor.submit(process_user, user_id) for user_id in user_ids]

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

            # Control de tasa (rate limiting)
            if len(results) % API_LIMIT == 0:
                elapsed = time.time() - start_time
                if elapsed < 1:
                    time.sleep(1 - elapsed)
                start_time = time.time()

            # Insertar en la base de datos en lotes
            if len(results) >= BATCH_SIZE:
                batch_insert_to_db(results)
                results.clear()

    # Insertar los datos restantes
    if results:
        batch_insert_to_db(results)

    print('Datos insertados en la base de datos.')
    
    # Carga de enlaces de Wikipedia
    
    print('Cargando enlaces de Wikipedia en la base de datos...')
    
    with Session(engine) as session:
        db_utils.insert_bulk_data(session, db_utils.get_wiki_data(), 'airportwikilink', 'airport_id', 'wikipedia_link')
        
    print('Enlaces de Wikipedia cargados en la base de datos.')
    
    # Creación de índices
    
    with Session(engine) as session:
        db_utils.create_index(session, "nearestairport", "user_id")
        db_utils.create_index(session, "airportwikilink", "airport_id")
        
    print('Índices creados.')
