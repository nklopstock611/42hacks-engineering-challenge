from sqlmodel import SQLModel, Field

class NearestAirport(SQLModel, table=True):
    """
    Represents the nearest airport for a user.
    It has a unique user_id and a non-unique airport_id.
    """
    user_id: int = Field(unique=True, nullable=False, primary_key=True)
    airport_id: int = Field(nullable=False)
    
class AirportWikiLink(SQLModel, table=True):
    """
    Combines each airport with its Wikipedia page link.
    It has a unique airport_id and a non-nullable wikipedia_link.
    """
    airport_id: int = Field(unique=True, nullable=False, primary_key=True)
    wikipedia_link: str = Field(nullable=False)
    