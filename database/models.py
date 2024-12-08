from sqlmodel import SQLModel, Field
from typing import Optional

class NearestAirport(SQLModel, table=True):
    user_id: int = Field(unique=True, nullable=False, primary_key=True)
    airport_id: int = Field(nullable=False)