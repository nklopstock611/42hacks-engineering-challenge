import time
import pandas
from sqlmodel import Session, SQLModel, create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed

import config as config
import app.utils as app_utils
import database.utils as db_utils

API_LIMIT = 60  # Request limit per second
BATCH_SIZE = 1000  # Batch size for database insert
RETRY_LIMIT = 3  # Number of retries before giving up

engine = create_engine(
    config.DB_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
)

def create_db_and_tables(engine_):
    SQLModel.metadata.create_all(engine_)

def process_user(user_id):
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            airport_id = app_utils.find_nearest_airport(user_id)['airport_id']
            print(f"User {user_id} processed: Nearest airport {airport_id}")
            return {"user_id": user_id, "airport_id": airport_id}
        except Exception as e:
            print(f"Attempt {attempt} failed for user {user_id}: {e}")
            time.sleep(1)  # wait before retrying
    print(f"User {user_id} failed after {RETRY_LIMIT} attempts.")
    return None

def batch_insert_to_db(data_batch):
    try:
        with Session(engine) as session:
            db_utils.insert_bulk_data(session, data_batch, 'nearestairport', 'user_id', 'airport_id')
        print(f"Batch with containing {len(data_batch)} users inserted into data base.")
    except Exception as e:
        print(f"Error when inserting into data base: {e}")

if __name__ == "__main__":
    # ============= #
    # CSV FILTERING #
    # ============= #
    
    print('Filtering airports...')
    
    df = pandas.read_csv('data/airports.csv')
    df = db_utils.filter_airports(df)
    df.to_csv('data/airports_w_wiki.csv', index=False)
    
    print('Airports filtered and new CSV filed created.')
    
    # =============== #
    # SCHEMA CREATION #
    # =============== #
    
    print('Creating schema...')
    create_db_and_tables(engine)
    print('Schema created.')

    # ================================== #
    # USER - NEAREST AIRPORT TABLES LOAD #
    # ================================== #

    print('Loading nearest airports into the database...')

    user_ids = range(0, 100000) # for all 100_000 users
    results = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=API_LIMIT) as executor:
        futures = [executor.submit(process_user, user_id) for user_id in user_ids]

        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

            # rate limit control
            if len(results) % API_LIMIT == 0:
                elapsed = time.time() - start_time
                if elapsed < 1:
                    time.sleep(1 - elapsed)
                start_time = time.time()

            # inserts in batches
            if len(results) >= BATCH_SIZE:
                batch_insert_to_db(results)
                results.clear()

    # insert remaining results
    if results:
        batch_insert_to_db(results)

    print('Data successfully loaded into the database.')
    
    # ==================================== #
    # AIRPORT - WIKIPEDIA LINKS TABLE LOAD #
    # ==================================== #
    
    print('Loading Wikipedia links into the database...')
    
    with Session(engine) as session:
        db_utils.insert_bulk_data(session, db_utils.get_wiki_data(), 'airportwikilink', 'airport_id', 'wikipedia_link')
        
    print('Wikipedia links successfully loaded into the database.')
    
    # ================ #
    # Indexes creation #
    # ================ #
    
    print('Creating indexes...')
    
    with Session(engine) as session:
        # an index for the user_id column in the nearestairport table -> reduces the time to find the wanted user
        db_utils.create_index(session, "nearestairport", "user_id")
        
        # an index for the airport_id column in the airportwikilink table -> reduces the time to find the wanted airport
        db_utils.create_index(session, "airportwikilink", "airport_id")
        
    print('Indexes successfully created.')
