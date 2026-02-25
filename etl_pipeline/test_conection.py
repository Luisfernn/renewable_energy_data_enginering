from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgres:postgres123@localhost:5432/renewable_energy"

try:
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()[0]
        print("✅ CONECTADO COM SUCESSO!")
        print(f"📊 PostgreSQL: {version}")

except Exception as e:
    print(f"❌ Erro na conexão {e}")
