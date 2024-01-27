import os
from sqlalchemy import create_engine, text
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

# Criação da engine do SQLAlchemy para o data warehouse
engine = create_engine(string_conexao_dw)

# Caminhos dos arquivos SQL
arquivo_sql_dim_tempo = diretorio_do_script+'/'+config['Paths']['arquivo_sql_dim_tempo']
arquivo_sql_dim_data = diretorio_do_script+'/'+config['Paths']['arquivo_sql_dim_data']



# Função para verificar se uma tabela existe no banco de dados
def tabela_existe(engine, nome_tabela):
    with engine.connect() as conexao:
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{nome_tabela}')"
        resultado = conexao.execute(text(query)).fetchone()
        return resultado[0]

# Função para executar um script SQL de um arquivo
def executar_script_arquivo(engine, arquivo_sql):
    with engine.connect() as conexao:
        with open(arquivo_sql, 'r') as arquivo:
            script_sql = arquivo.read()
            print(text(script_sql))
            conexao.execute(text(script_sql))




# Verificar e executar scripts SQL se as tabelas não existirem
if not tabela_existe(engine, 'dim_horas'):
    executar_script_arquivo(engine, arquivo_sql_dim_tempo)

if not tabela_existe(engine, 'dim_calendario'):
    executar_script_arquivo(engine, arquivo_sql_dim_data)

# Fechar a conexão com o banco de dados
engine.dispose()

print("Scripts executados com sucesso.")
