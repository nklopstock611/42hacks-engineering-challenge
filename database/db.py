import database.utils as db_utils
import app.utils as app_utils

from sqlmodel import Session, SQLModel, create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

engine = create_engine("postgresql+psycopg2://postgres.hdpfvpoeoyjressnyhqx:42hacks2024!@aws-0-us-west-1.pooler.supabase.com:6543/postgres")

def create_db_and_tables(engine_):
    SQLModel.metadata.create_all(engine_)

def get_db():
    with Session(engine) as session:
        yield session
        
def process_user(user_id):
    try:
        with Session(engine) as session:
            airport_id = app_utils.find_nearest_airport(user_id)['airport_id']
            db_utils.insert_data(session, user_id, airport_id)
            return f"Procesado usuario {user_id}"
    except Exception as e:
        return f"Error procesando usuario {user_id}: {e}"

if __name__ == "__main__":
    create_db_and_tables(engine)
    with ThreadPoolExecutor(max_workers=app_utils.API_LIMIT) as executor:
        futures = [executor.submit(process_user, i) for i in range(99_999)]

        for future in as_completed(futures):
            print(future.result())

        # utils.create_user_id_index(session)
        # utils.get_user_airport(session, i)