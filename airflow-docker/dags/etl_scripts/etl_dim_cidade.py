import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, exists, Date, MetaData, Table, ForeignKey, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timedelta

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

# Criação da tabela da dimensão Cidade
Base = declarative_base()

class DimensaoCidade(Base):
    __tablename__ = 'dim_cidade'
    sk_cidade = Column(Integer, primary_key=True)
    nk_cidade = Column(Integer)
    nm_cidade = Column(String)
    uf = Column(String)
    ddd = Column(Integer)
    data_inicio = Column(Date)
    data_fim = Column(Date)

# Criação da tabela se não existir
Base.metadata.create_all(engine_dw)

# Criação da sessão do SQLAlchemy
Session = sessionmaker(bind=engine_dw)
session_dw = Session()
inspector = inspect(engine_dw)

# Verifica se a cidade '<não especificada>' já existe na dimensão
dp_n_especificado_exists = session_dw.query(exists().where(DimensaoCidade.sk_cidade == -1)).scalar()

# Adiciona o registro padrão se não existir
if not dp_n_especificado_exists:
    cidade_n_especificada = DimensaoCidade(sk_cidade=-1,
            nk_cidade=00,
            nm_cidade='<não especificado>',
            uf='<não especificado>',
            ddd=00,
            data_inicio=datetime(day=1,year=1900,month=1),
            data_fim=None)
    session_dw.add(cidade_n_especificada)
    session_dw.commit()

# Leitura da tabela de cidades_dl do banco principal
engine_dl = create_engine(string_conexao_dl)
query_cidades_dl = """SELECT "Cidade Principal", "UF", "DDD"::varchar 
	FROM public.cidades
	WHERE "DDD" IS NOT NULL
	ORDER BY "Cidade Principal";"""
df_cidades_dl = pd.read_sql_query(query_cidades_dl, engine_dl)


# Remover nulos
df_cidades_dl['Cidade Principal'].dropna()


# Consulta para obter os cidades existentes na dimensão
query_cidade_dimensao = session_dw.query(DimensaoCidade).distinct()
try:
    df_cidades_dw = pd.read_sql_query(query_cidade_dimensao, engine_dw)
except Exception as e:
    pass

 
if df_cidades_dl.size > 0 :
    for indice, cidade_dl in df_cidades_dl.iterrows():
        
        print(cidade_dl)

        # Verifica se a cidade já existe na dimensão
        cidade_existente = session_dw.query(DimensaoCidade).filter_by(ddd=cidade_dl['DDD']).first()

        # Se a cidade já existir, encerra a vigência no dia anterior e cria uma nova entrada
        if cidade_existente:
            cidade_existente.data_fim = datetime.strptime(cidade_dl['data_inicio'], '%Y-%m-%d').date() - timedelta(days=1)
        
            
            ova_cidade = DimensaoCidade(
                nk_cidade=cidade_dl['DDD'],
                nm_cidade=cidade_dl['Cidade Principal'],
                uf=cidade_dl['UF'],
                ddd=cidade_dl['DDD'],
                data_inicio=datetime.strptime(cidade_dl['data_inicio'], '%Y-%m-%d').date() ,
                data_fim=None
            )

            session_dw.add(nova_cidade)
        
        # Se a cidade não existir, adiciona uma nova entrada
        else:
            
            nova_cidade = DimensaoCidade(
                nk_cidade=cidade_dl['DDD'],
                nm_cidade=cidade_dl['Cidade Principal'],
                uf=cidade_dl['UF'],
                ddd=cidade_dl['DDD'],
                data_inicio=datetime(day=1,year=1900,month=1),
                data_fim=None
            )

            session_dw.add(nova_cidade)
      
 
    
          
# Commit para salvar as alterações
session_dw.commit()

# Encerramento da sessão
session_dw.close()
