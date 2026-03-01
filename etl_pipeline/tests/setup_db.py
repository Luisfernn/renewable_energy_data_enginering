from sqlalchemy import create_engine, text
from pathlib import Path

# String de conexão
DATABASE_URL = 'postgresql+psycopg2://postgres:postgres123@localhost:5433/renewable_energy'

# Caminho do SQL
BASE_DIR = Path(__file__).parent
SQL_FILE = BASE_DIR / 'sql' / 'create_tables.sql'

def create_tables():
    print("🔧 Criando tabelas no PostgreSQL...")
    
    try:
        # Conecta
        engine = create_engine(DATABASE_URL)
        
        # Lê SQL
        with open(SQL_FILE, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        # Executa
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        
        print("✅ Tabelas criadas com sucesso!")
        print("📊 Tabelas: dim_country, dim_technology, dim_time, dim_producer, fact_energy_generation")
        
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")

if __name__ == "__main__":
    create_tables()