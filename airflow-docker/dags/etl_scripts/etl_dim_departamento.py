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

# Criação da tabela da dimensão Departamento
Base = declarative_base()

class DimensaoDepartamento(Base):
    __tablename__ = 'dim_departamento'
    sk_departamento = Column(Integer, primary_key=True)
    nm_departamento = Column(String(255))

# Criação da tabela se não existir
Base.metadata.create_all(engine_dw)

# Criação da sessão do SQLAlchemy
Session = sessionmaker(bind=engine_dw)
session_dw = Session()

# Verifica se o depatamento '<não especificado>' já existe na dimensão
dp_n_especificado_exists = session_dw.query(exists().where(DimensaoDepartamento.nm_departamento == '<não especificado>')).scalar()

# Adiciona o registro padrão se não existir
if not dp_n_especificado_exists:
    departamento_dimensao = DimensaoDepartamento(nm_departamento='<não especificado>', sk_departamento=-1)
    session_dw.add(departamento_dimensao)
    session_dw.commit()

# Leitura da tabela de atendimentos do banco principal
engine_dl = create_engine(string_conexao_dl)
query_atendimentos = """SELECT DISTINCT "Departamento" FROM public.atendimentos WHERE "Departamento" IS NOT NULL ORDER BY "Departamento" """
df_atendimentos = pd.read_sql_query(query_atendimentos, engine_dl)

# Remover nulos
df_atendimentos['Departamento'].dropna()

# Consulta para obter os departamentos existentes na dimensão
query_departamento_dimensao = session_dw.query(DimensaoDepartamento.nm_departamento).distinct()
departamento_dimensao = [departamento[0] for departamento in query_departamento_dimensao]

# Identificação de novos departamentos e departamentos existentes
novos_departamentos = df_atendimentos[~df_atendimentos['Departamento'].isin(departamento_dimensao)]['Departamento'].unique()
departamentos_existentes = df_atendimentos[df_atendimentos['Departamento'].isin(departamento_dimensao)]['Departamento'].unique()

# Inserção de novos departamentos na dimensão
for novo_departamento in novos_departamentos:
    departamento_dimensao = DimensaoDepartamento(nm_departamento=novo_departamento)
    session_dw.add(departamento_dimensao)

# Commit para salvar as alterações
session_dw.commit()

# Atualização de departamentos existentes na dimensão
for departamento_existente in departamentos_existentes:
    query_departamento = session_dw.query(DimensaoDepartamento).filter_by(nm_departamento=departamento_existente).first()
    if query_departamento:
        # Atualiza o departamento na dimensão se necessário
        pass

# Commit para salvar as alterações
session_dw.commit()

# Encerramento da sessão
session_dw.close()
