# 1. Imagem base oficial do Python
FROM python:3.11-slim

# 2. Instala dependências do sistema para o banco de dados
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 3. Define onde o código vai ficar dentro do container
WORKDIR /app

# 4. Instala as bibliotecas do projeto
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todo o código para dentro do container
COPY . .

# 6. O comando que roda o ETL
CMD ["python", "scr/main.py"]

# 7. Variável de ambiente para garantir que os logs sejam exibidos em tempo real
ENV PYTHONUNBUFFERED=1