import time
from sqlmodel import Session, SQLModel, create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

import database.utils as db_utils
import app.utils as app_utils

API_LIMIT = 60  # Límite de solicitudes por segundo
BATCH_SIZE = 1000  # Tamaño de los lotes para la base de datos
RETRY_LIMIT = 3  # Número de reintentos en caso de error

engine = create_engine("postgresql+psycopg2://postgres.hdpfvpoeoyjressnyhqx:42hacks2024!@aws-0-us-west-1.pooler.supabase.com:6543/postgres")

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
            db_utils.insert_bulk_data(session, data_batch)
        print(f"Lote de {len(data_batch)} usuarios insertado en la base de datos.")
    except Exception as e:
        print(f"Error al insertar el lote en la base de datos: {e}")

if __name__ == "__main__":
    create_db_and_tables(engine)
    print('Base de datos y tablas creadas.')

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

    with Session(engine) as session:
        db_utils.create_user_id_index(session)
        
    print('Índice creado en la base de datos.')
