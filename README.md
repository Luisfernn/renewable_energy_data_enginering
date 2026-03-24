# 🌱 Renewable Energy Data Engineering
 
Pipeline ETL para processamento de dados de energia renovável da [IRENA](https://www.irena.org/) (International Renewable Energy Agency), com foco em boas práticas de Engenharia de Dados.
 
---
 
## 📋 Sobre o Projeto
 
Este projeto foi desenvolvido como portfólio profissional em Engenharia de Dados. O pipeline extrai dados públicos de energia renovável da IRENA, realiza transformações estruturadas e carrega os resultados em um banco de dados PostgreSQL — tudo orquestrado via Docker.
 
O objetivo é demonstrar domínio de práticas reais do mercado: código limpo e modular, logging estruturado, commits convencionais, separação de responsabilidades e ambiente reproducível com containers.
 
---
 
## 🏗️ Arquitetura do Pipeline
 
```
IRENA (fonte xlsx)
        │
        ▼
   [ Extract ]
   Leitura dos arquivos com openpyxl/pandas
        │
        ▼
  [ Transform ]
   Limpeza, normalização e separação
   de dados textuais e numéricos
        │
        ▼
    [ Load ]
   Persistência no PostgreSQL via SQLAlchemy
        │
        ▼
  PostgreSQL + Docker
```
 
---
 
## 🗂️ Estrutura do Projeto
 
```
renewable_energy_data_enginering/
│
├── etl_pipeline/
│   ├── data/
│   │   ├── logs/
│   │   │   └── etl.log
│   │   ├── processed/
│   │   │   ├── renewable_energy_data_clean.csv
│   │   │   ├── renewable_energy_data_final.csv
│   │   │   ├── renewable_energy_data_text.csv
│   │   │   └── renewable_energy_data_valida.csv
│   │   └── raw/
│   │       ├── renewable_energy_data_raw.xlsx
│   │       └── renewable_energy_data.csv
│   ├── scr/
│   │   ├── transform/
│   │   │   ├── numeric.py
│   │   │   └── text.py
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── main.py
│   │   └── validation.py
│   ├── sql/
│   │   └── create_tables.sql
│   └── tests/
│       ├── setup_db.py
│       └── test_conection.py
│
├── .dockerignore
├── .env
├── .gitignore
├── config.py
├── docker-compose.yml
├── Dockerfile
├── LICENSE
├── README.md
└── requirements.txt
```
 
---
 
## 🛠️ Tecnologias Utilizadas
 
| Categoria        | Tecnologia                              |
|------------------|-----------------------------------------|
| Linguagem        | Python 3.12.7                           |
| Manipulação      | Pandas, NumPy                           |
| Leitura de dados | openpyxl                                |
| Banco de dados   | PostgreSQL                              |
| ORM              | SQLAlchemy                              |
| Containerização  | Docker / Docker Compose                 |
| Versionamento    | Git + Conventional Commits              |
 
---
 
## ⚙️ Como Executar
 
### Pré-requisitos
 
- [Docker](https://www.docker.com/) e Docker Compose instalados
- Python 3.11+ (para execução local sem Docker)
 
### 1. Clone o repositório
 
```bash
git clone https://github.com/Luisfernn/renewable_energy_data_enginering.git
cd renewable_energy_data_enginering
```
 
### 2. Configure as variáveis de ambiente
 
Crie um arquivo `.env` na raiz do projeto:
 
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=sua_senha
POSTGRES_DB=renewable_energy
POSTGRES_HOST=db
POSTGRES_PORT=5432
```
 
### 3. Suba o ambiente com Docker
 
```bash
docker-compose up --build
```
 
O Docker irá subir o PostgreSQL e executar o pipeline automaticamente.
 
### 4. Execução local (sem Docker)
 
```bash
pip install -r requirements.txt
python etl_pipeline/main.py
```
 
---
 
## 📊 Fonte de Dados
 
Os dados são provenientes da **IRENA — International Renewable Energy Agency**, uma organização intergovernamental que apoia países na transição para energias renováveis.
 
- 🔗 [IRENA Data & Statistics](https://www.irena.org/Publications/2025/Jul/Renewable-energy-statistics-2025)
- Formato: arquivos `.xlsx`
- Cobertura: capacidade instalada e geração de energia renovável por país e por fonte (solar, eólica, hidroelétrica, etc.)
 
---
 
## 📐 Boas Práticas Aplicadas
 
- **Conventional Commits** — histórico de commits padronizado e legível
- **Logging estruturado** — rastreabilidade de cada etapa do pipeline
- **Separação de responsabilidades** — extract, transform e load em módulos independentes
- **Transformações distintas** — scripts separados para dados textuais e numéricos
- **Código pythônico** — snake_case, pathlib, type hints
- **Ambiente reproducível** — Docker garante paridade entre dev e produção
- **`.gitignore` cuidadoso** — dados brutos e credenciais fora do repositório
 
---
 
## 📄 Licença
 
Este projeto está licenciado sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para mais detalhes.
 
---
 
## 👤 Autor
 
**Luis Fernando**
- GitHub: [@Luisfernn](https://github.com/Luisfernn)