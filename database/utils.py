import os
import csv
import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy.orm import Session

def filter_airports(dataframe: pd.DataFrame, column_name='wikipedia_link'):
    """    
    Filters the rows of the "dataframe" dataset that do not have a value in the "column_name" column.
    In this particular case, it filters the airports that do not have a link to Wikipedia.
    """
    filtered_df = dataframe[dataframe[column_name].notna() & (dataframe[column_name] != '')]
    
    return filtered_df

def insert_bulk_data(session: Session, data_batch: list, table: str, col1: str, col2: str, temp_file="temp_data.csv"):
    """
    Inserts a batch of data into "table" using the PostgreSQL COPY command.
    """
    if not data_batch:
        print("No data to insert.")
        return

    # uses a temp CSV file to store the data
    try:
        with open(temp_file, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([col1, col2])
            for record in data_batch:
                writer.writerow([record[col1], record[col2]])

        print(f"Temp file created with {len(data_batch)} rows.")
        
        # uses the COPY instruction to insert data from the CSV file
        # This instruction is more efficient for inserting large volumes of data
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
        print(f"{len(data_batch)} inserted rows into {table}.")
    except Exception as e:
        session.rollback()
        print(f"Error while inserting in bulk mode: {e}")
    finally:
        # removes the temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)

def get_wiki_data(airports_file="data/airports_w_wiki.csv") -> list:
    """
    Returns an array of dictionaries with the data of airports and their Wikipedia links.
    """
    airports = pd.read_csv(airports_file)
    data = [ { "airport_id": row["id"], "wikipedia_link": row["wikipedia_link"] } for _, row in airports.iterrows() ]
    
    return data

def create_index(session: Session, table: str, column: str):
    """
    Creates an index on one column in a table.
    """
    index_name = f"idx_{column}"
    try:
        query = f"""
            CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column});
        """
        session.execute(text(query))
        session.commit()
        print("Index created successfully.")
    except Exception as e:
        print(f"Error when creating index: {e}")
        session.rollback()

def get_user_airport(session: Session, user_id: int) -> int:
    """
    Query to get the nearest airport to a user.
    """
    query = text("""
        SELECT airport_id 
        FROM nearestairport 
        WHERE user_id = :user_id
    """)
    result = session.execute(query, { "user_id": user_id }).fetchone()
    
    return result[0] if result else None

def get_airports_wiki_link(session: Session, airport_id: int) -> str:
    """
    Query to get the Wikipedia link of an airport.
    """
    query = text("""
        SELECT wikipedia_link 
        FROM airportwikilink
        WHERE airport_id = :airport_id
    """)
    result = session.execute(query, { "airport_id": airport_id }).fetchone()
    
    return result[0] if result else None
