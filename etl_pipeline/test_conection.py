import psycopg2
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgres:postgres123@localhost:5432/renewable_energy"

# Test 1: direct psycopg2 connection
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="renewable_energy",
        user="postgres",
        password="postgres123",
        options="-c client_encoding=UTF8",
    )
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    cur.close()
    conn.close()
    print("Psycopg2 OK:", version)
except Exception as e:
    print("Psycopg2 error:", repr(e))

# Test 2: SQLAlchemy connection
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()[0]
    print("SQLAlchemy OK:", version)
    print("CONECTADO COM SUCESSO!")
except Exception as e:
    print("SQLAlchemy error:", repr(e))
