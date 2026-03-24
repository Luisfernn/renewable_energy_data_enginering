# 🌱 Renewable Energy Data Engineering
 
Pipeline ETL para processamento de dados de energia renovável da [IRENA](https://www.irena.org/) (International Renewable Energy Agency), com foco em boas práticas de Engenharia de Dados.
 
---
 
## 📋 Sobre o Projeto
 
Este projeto foi desenvolvido como portfólio profissional em Engenharia de Dados. O pipeline extrai dados públicos de energia renovável da IRENA, realiza transformações estruturadas e carrega os resultados em um banco de dados PostgreSQL orquestrado via Docker.
 
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
├── data/                  # Armazenamento de arquivos locais
│   ├── logs/              # Logs detalhados da execução do ETL
│   ├── processed/         # Dados limpos e transformados (CSV)
│   └── raw/               # Dados brutos originais (Extraídos da fonte)
├── sql/                   # Script SQL para criação do Schema no PostgreSQL
│   └── create_tables.sql  # Definição de tabelas Fato e Dimensões
├── src/                   # Código-fonte do Pipeline (Core)
│   ├── transform/         # Módulos específicos de transformação
│   │   ├── numeric.py     # Tratamento de métricas e outliers
│   │   └── text.py        # Padronização de strings e categorias
│   ├── extract.py         # Lógica de ingestão e leitura de arquivos
│   ├── load.py            # Carga no Data Warehouse (Star Schema)
│   ├── main.py            # Orquestrador principal do fluxo ETL
│   └── validation.py      # Camada de qualidade e integridade de dados
├── tests/                 # Testes de integração
├── .env                   # Variáveis de ambiente (Configurações sensíveis)
├── config.py              # Centralização de caminhos e variáveis globais
├── docker-compose.yml     # Orquestração dos containers (App + Database)
├── Dockerfile             # Receita para build da imagem Python
└── requirements.txt       # Dependências do projeto (Pandas, SQLAlchemy, etc.)
```
 
---
 
## 🛠️ Tecnologias Utilizadas
 
| Categoria        | Tecnologia                              |
|------------------|-----------------------------------------|
| Linguagem        | Python 3.11                             |
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
 
```# Configurações do Banco
DB_NAME=renewable_energy
DB_USER=postgres
DB_PASS=sua_senha_forte

# URLs de Conexão (Local vs Docker)
DATABASE_URL=postgresql://postgres:sua_senha_forte@localhost:5433/renewable_energy
DATABASE_URL_DOCKER=postgresql://postgres:sua_senha_forte@postgres:5432/renewable_energy
```
 
### 3. Suba o ambiente com Docker
 
```bash
docker-compose up --build
```
 
O Docker irá subir o PostgreSQL e executar o pipeline automaticamente.
 
### 4. Execução local (sem Docker)
 
```bash
pip install -r requirements.txt
python src/main.py
```
 
---
 
## 📊 Fonte de Dados
 
Os dados são provenientes da **IRENA — International Renewable Energy Agency**, uma organização intergovernamental que apoia países na transição para energias renováveis.
 
- 🔗 [Renewable Energy Statistics 2025](https://www.irena.org/Publications/2025/Jul/Renewable-energy-statistics-2025)
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

## 🚀 Destaques Técnicos
- **Star Schema:** Modelagem dimensional com tabelas Fato (`fact_energy_generation`) e Dimensões (`dim_country`, `dim_technology`, etc).
- **Atomicidade:** Carga realizada dentro de transações SQL (All-or-Nothing).
- **Idempotência:** O pipeline limpa dados antigos e reinicia as sequences antes de cada carga, permitindo múltiplas execuções sem duplicidade.
- **Ambiente Híbrido:** Lógica de `get_engine()` que detecta automaticamente se o código está rodando dentro ou fora do Docker.
 
## 📄 Licença
 
Este projeto está licenciado sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE) para mais detalhes.
 
---
 
## 👤 Autor
 
**Luis Fernando**
- GitHub: [@Luisfernn](https://github.com/Luisfernn)