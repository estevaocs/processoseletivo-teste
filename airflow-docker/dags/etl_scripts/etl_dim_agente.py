import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, exists
from sqlalchemy.orm import sessionmaker, declarative_base

import configparser

# Obtém o caminho do diretório do script
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))

# Leitura das configurações do banco de dados do arquivo .ini
config = configparser.ConfigParser()
config.read(diretorio_do_script+'/config.ini')

# Configuração do banco de dados do data warehouse
user = config.get('Database', 'user')
password = config.get('Database', 'password')
host = config.get('Database', 'host')
port = config.get('Database', 'port')
database_dw = config.get('Database', 'database_dw')
database_dl = config.get('Database', 'database_dl')

# String de conexão para o banco de dados do data warehouse e data lake
string_conexao_dw = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_dw}'
string_conexao_dl = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database_dl}'

# Criação da engine do SQLAlchemy para o data warehouse
engine_dw = create_engine(string_conexao_dw)

# Criação da tabela da dimensão Agente
Base = declarative_base()

class DimensaoAgente(Base):
    __tablename__ = 'dim_agente'
    sk_agente = Column(Integer, primary_key=True)
    nm_agente = Column(String(255))

# Criação da tabela se não existir
Base.metadata.create_all(engine_dw)

# Criação da sessão do SQLAlchemy
Session = sessionmaker(bind=engine_dw)
session_dw = Session()

# Verifica se o agente '<chatbot>' já existe na dimensão
chatbot_exists = session_dw.query(exists().where(DimensaoAgente.nm_agente == '<chatbot>')).scalar()

# Adiciona o registro padrão se não existir
if not chatbot_exists:
    chatbot_dimensao = DimensaoAgente(nm_agente='<chatbot>', sk_agente=-1)
    session_dw.add(chatbot_dimensao)
    session_dw.commit()

# Leitura da tabela de atendimentos do banco principal
engine_dl = create_engine(string_conexao_dl)
query_atendimentos = "SELECT DISTINCT agente FROM public.atendimentos WHERE agente IS NOT NULL ORDER BY agente"
df_atendimentos = pd.read_sql_query(query_atendimentos, engine_dl)

# Substitui nulos por '<chatbot>'
df_atendimentos['agente'].dropna()

# Consulta para obter os agentes existentes na dimensão
query_agentes_dimensao = session_dw.query(DimensaoAgente.nm_agente).distinct()
agentes_dimensao = [agente[0] for agente in query_agentes_dimensao]

# Identificação de novos agentes e agentes existentes
novos_agentes = df_atendimentos[~df_atendimentos['agente'].isin(agentes_dimensao)]['agente'].unique()
agentes_existentes = df_atendimentos[df_atendimentos['agente'].isin(agentes_dimensao)]['agente'].unique()

# Inserção de novos agentes na dimensão
for novo_agente in novos_agentes:
    agente_dimensao = DimensaoAgente(nm_agente=novo_agente)
    session_dw.add(agente_dimensao)

# Commit para salvar as alterações
session_dw.commit()

# Atualização de agentes existentes na dimensão
for agente_existente in agentes_existentes:
    query_agente = session_dw.query(DimensaoAgente).filter_by(nm_agente=agente_existente).first()
    if query_agente:
        # Atualiza o agente na dimensão se necessário
        pass

# Commit para salvar as alterações
session_dw.commit()

# Encerramento da sessão
session_dw.close()
