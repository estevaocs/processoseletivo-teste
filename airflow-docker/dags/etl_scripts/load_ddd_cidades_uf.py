import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import configparser

# Obtém o caminho do diretório do script
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))

# Leitura das configurações do arquivo INI
config = configparser.ConfigParser()
config.read(diretorio_do_script+'/config.ini')

# Configurações do banco de dados
usuario = config.get('Database', 'user')
senha = config.get('Database', 'password')
host = config.get('Database', 'host')
porta = config.get('Database', 'port')
nome_banco = config.get('Database', 'database_dl')

# String de conexão
string_conexao = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{nome_banco}'

# Caminho para o arquivo XLSX
caminho_arquivo_xlsx = diretorio_do_script+'/'+config.get('Paths', 'xlsx_ddd_cidades_uf')

# Nome da tabela no banco de dados
nome_tabela = 'cidades'

# Leitura do XLSX para um DataFrame do pandas
try:
    df = pd.read_excel(caminho_arquivo_xlsx)
except Exception as e:
    # Loga a mensagem de erro
    print(f'arquivo não encontrado na pasta de origem {caminho_arquivo_xlsx}: {e}')
    # Encerra o programa
    sys.exit()
    
    
# Criação da engine do SQLAlchemy
engine = create_engine(string_conexao)

# Carregamento dos dados para o PostgreSQL
try:
    df.to_sql(nome_tabela, engine, if_exists='replace', index=False)

    print(f'Dados inseridos na tabela {nome_tabela} com sucesso.')

except Exception as e:
    print(f'Erro ao inserir dados: {e}')

finally:
    # Fecha a conexão com o banco de dados
    engine.dispose()