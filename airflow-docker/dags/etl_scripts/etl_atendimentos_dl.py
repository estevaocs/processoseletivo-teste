import configparser
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import logging
import shutil

# Obtém o caminho do diretório do script
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))

# Configuração do logger
logging.basicConfig(filename=diretorio_do_script+'/processamento.log', level=logging.INFO)

# Leitura das configurações do arquivo INI
config = configparser.ConfigParser()
config.read(diretorio_do_script+'/config.ini')


# Configurações do banco de dados
usuario = config.get('Database', 'user')
senha = config.get('Database', 'password')
host = config.get('Database', 'host')
porta = config.get('Database', 'port')
nome_banco = config.get('Database', 'database_dl')

# Configurações de caminho
pasta_origem = diretorio_do_script+'/'+config.get('Paths', 'origem')
pasta_destino = diretorio_do_script+'/'+config.get('Paths', 'destino')

# String de conexão
string_conexao = f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{nome_banco}'

# Lista todos os arquivos CSV na pasta de origem
arquivos_csv = [f for f in os.listdir(pasta_origem) if f.endswith('.csv')]

# Verifica se há arquivos CSV
if not arquivos_csv:
    print("Nenhum arquivo CSV encontrado na pasta de processamento.")
    
    # Loga a mensagem de erro
    logging.error("Nenhum arquivo CSV encontrado na pasta de processamento.")
    
    # Encerra o programa
    sys.exit()


# Criação da engine do SQLAlchemy
engine = create_engine(string_conexao)

# Função para processar e carregar um arquivo CSV
def processar_e_carregar_csv(caminho_arquivo_csv):
    try:
        # Leitura do CSV para um DataFrame do pandas
        df = pd.read_csv(caminho_arquivo_csv)

        # Carregamento dos dados para o PostgreSQL
        nome_tabela = 'atendimentos'
        df.to_sql(nome_tabela, engine, if_exists='replace', index=False)

        logging.info(f'Dados do arquivo {caminho_arquivo_csv} inseridos na tabela {nome_tabela} com sucesso.')

        # Move o arquivo CSV para a pasta de arquivos processados
        shutil.move(caminho_arquivo_csv, os.path.join(pasta_destino, os.path.basename(caminho_arquivo_csv)))

    except Exception as e:
        logging.error(f'Erro ao processar dados do arquivo {caminho_arquivo_csv}: {e}')


# Loop sobre todos os arquivos CSV na pasta de origem
for arquivo_csv in arquivos_csv:
    caminho_arquivo_csv = os.path.join(pasta_origem, arquivo_csv)
    processar_e_carregar_csv(caminho_arquivo_csv)

# Fecha a conexão com o banco de dados
engine.dispose()
