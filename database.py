from sqlalchemy import create_engine

PG_CONN = "postgresql+psycopg2://postgres:12345@localhost:5432/cbs_data"

def get_engine():
    engine = create_engine(PG_CONN)
    return engine
