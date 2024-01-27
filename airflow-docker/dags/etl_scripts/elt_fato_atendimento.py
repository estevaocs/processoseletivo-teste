from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, exists, text
from sqlalchemy.orm import sessionmaker, declarative_base
import configparser

# Obtém o caminho do diretório do script
diretorio_do_script = os.path.dirname(os.path.abspath(__file__))

# Leitura das configurações do arquivo INI
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

engine_dw = create_engine(string_conexao_dw)

# Criação da engine do SQLAlchemy para o data lake
engine_dl = create_engine(string_conexao_dl)

# Criação da tabela da dimensão Departamento
Base = declarative_base()

# Pegando o último registro
query_ultimo_resgistro = '''SELECT data_criacao, hora_criacao
FROM public.ft_atendimentos
ORDER BY data_criacao DESC, hora_criacao DESC
LIMIT 1;
;
'''
df_ultimo_registro = pd.read_sql_query(query_ultimo_resgistro, engine_dw)

if not df_ultimo_registro['data_criacao'].empty:
    data_ultimo_registro = df_ultimo_registro['data_criacao'][0].strftime(
        '%Y-%m-%d')
    hora_ultimo_registro = df_ultimo_registro['hora_criacao'][0].strftime(
        '%H:%M')


    # Deletando possiveis atualizações
    with engine_dw.connect() as conexao:
        
        query_atendimentos_delete = f'''
        DELETE FROM public.ft_atendimentos
            WHERE (data_criacao >= CAST('{data_ultimo_registro}' AS date)
            and  cast(hora_criacao as time) > cast('{hora_ultimo_registro}' as time)) OR
            data_criacao > CAST('{data_ultimo_registro}' AS date)
        '''
        conexao.execute(text(query_atendimentos_delete))
        
        
    
else:
    data_ultimo_registro = datetime.fromtimestamp(0)
    hora_ultimo_registro = datetime.fromtimestamp(0)


# Consultar dados de atendimentos da tabela no Data Lake
query_atendimentos = f'''
SELECT * FROM public.atendimentos WHERE (TO_DATE(data_criacao, 'DD/MM/YYYY') >= CAST('{data_ultimo_registro}' AS date)
    and  cast(hora_criacao as time) > cast('{hora_ultimo_registro}' as time)) OR
    TO_DATE(data_criacao, 'DD/MM/YYYY') > CAST('{data_ultimo_registro}' AS date)
'''
df_atendimentos = pd.read_sql_query(query_atendimentos, engine_dl)

##### TRANSFORMAÇÕES ######
# Adicionar coluna de DDD à dimensão cidade
df_atendimentos['ddd_cidade'] = df_atendimentos['telefone'].str[:2]

# Mapear DDD para cidade (usando a dimensão cidade)
df_cidades = pd.read_sql_query('SELECT * FROM dim_cidade', engine_dw)

# Converter a coluna 'ddd_cidade' para o tipo int64
df_atendimentos['ddd_cidade'] = df_atendimentos['ddd_cidade'].astype('int64')
df_atendimentos_fato = df_atendimentos.merge(
    df_cidades, left_on='ddd_cidade', right_on='ddd', how='left')

# Mapear agente
df_agentes = pd.read_sql_query('SELECT * FROM dim_agente', engine_dw)
df_atendimentos_fato['agente'] = df_atendimentos_fato['agente'].fillna(
    '<chatbot>')
df_atendimentos_fato = df_atendimentos_fato.merge(
    df_agentes, left_on='agente', right_on='nm_agente', how='left')

# Transformando as datas e horas
df_atendimentos_fato.data_transferencia = pd.to_datetime(
    df_atendimentos_fato.data_transferencia, format="%d/%m/%Y %H:%M:%S").dt.date
df_atendimentos_fato.data_finalizacao = pd.to_datetime(
    df_atendimentos_fato.data_finalizacao, format="%d/%m/%Y %H:%M:%S").dt.date
df_atendimentos_fato.data_criacao = pd.to_datetime(
    df_atendimentos_fato.data_criacao, format="%d/%m/%Y").dt.date
df_atendimentos_fato.data_ultima_msg_agente = pd.to_datetime(
    df_atendimentos_fato.data_ultima_msg_agente, format="%d/%m/%Y %H:%M:%S").dt.date
df_atendimentos_fato.data_ultima_msg_cliente = pd.to_datetime(
    df_atendimentos_fato.data_ultima_msg_cliente, format="%d/%m/%Y %H:%M:%S").dt.date
df_atendimentos_fato.hora_ultima_msg_agente = pd.to_datetime(
    df_atendimentos_fato.hora_ultima_msg_agente, format="%H:%M").dt.time
df_atendimentos_fato.hora_ultima_msg_cliente = pd.to_datetime(
    df_atendimentos_fato.hora_ultima_msg_cliente, format="%H:%M").dt.time
df_atendimentos_fato.hora_criacao = pd.to_datetime(
    df_atendimentos_fato.hora_criacao, format="%H:%M").dt.time
df_atendimentos_fato.hora_finalizacao = pd.to_datetime(
    df_atendimentos_fato.hora_finalizacao, format="%H:%M").dt.time
df_atendimentos_fato.hora_transferencia = pd.to_datetime(
    df_atendimentos_fato.hora_transferencia, format="%H:%M").dt.time

# Mapear departamento
df_departamentos = pd.read_sql_query(
    'SELECT * FROM dim_departamento', engine_dw)
df_atendimentos_fato = df_atendimentos_fato.merge(
    df_departamentos, left_on='Departamento', right_on='nm_departamento', how='left')

######## CALCULOS DE MÉTRICAS #############

# Calculando o tempo de atendimento total
tempo_de_atendimentos_horas = pd.to_timedelta(
    df_atendimentos_fato.tempo_de_atendimento_horas, 'h')
tempo_de_atendimento_minutos = pd.to_timedelta(
    df_atendimentos_fato.tempo_de_atendimento_minutos, 'm')
tempo_de_atendimento_segundos = pd.to_timedelta(
    df_atendimentos_fato.tempo_de_atendimento_segundos, 's')

total_tempo_de_atendimentos = tempo_de_atendimentos_horas + \
    tempo_de_atendimento_minutos + tempo_de_atendimento_segundos

df_atendimentos_fato['tempo_de_atendimentos'] = total_tempo_de_atendimentos.dt.total_seconds()

# Calculando o tempo de atendimento humano
tempo_de_atendimento_humano_horas = pd.to_timedelta(
    df_atendimentos_fato.tempo_de_atendimento_humano_horas, 'h')
tempo_de_atendimento_humano_minutos = pd.to_timedelta(
    df_atendimentos_fato.tempo_de_atendimento_humano_minutos, 'm')
tempo_de_atendimento_humano_segundos = pd.to_timedelta(
    df_atendimentos_fato.tempo_de_atendimento_humano_segundos, 's')

tempo_de_atendimento_humano = tempo_de_atendimento_humano_horas + tempo_de_atendimento_humano_minutos + tempo_de_atendimento_humano_segundos

df_atendimentos_fato['tempo_de_atendimento_humano'] = tempo_de_atendimento_humano.dt.total_seconds()

# Calculando o tempo de atendimento automatizado
tempo_atendimento_automatizado = total_tempo_de_atendimentos - \
    tempo_de_atendimento_humano
df_atendimentos_fato['tempo_atendimento_automatizado'] = tempo_atendimento_automatizado.dt.total_seconds() 

# Calculando o tempo de espera
tempo_em_espera_horas = pd.to_timedelta(
    df_atendimentos_fato.tempo_em_espera_horas, 'h')
tempo_em_espera_minutos = pd.to_timedelta(
    df_atendimentos_fato.tempo_em_espera_minutos, 'm')
tempo_em_espera_segundos = pd.to_timedelta(
    df_atendimentos_fato.tempo_em_espera_segundos, 's')

tempo_em_espera = tempo_em_espera_horas + \
    tempo_em_espera_minutos + tempo_em_espera_segundos

df_atendimentos_fato['tempo_de_espera'] = tempo_em_espera.dt.total_seconds(
) / 60.0

############ LIMPEZA DE DADOS #############

# Removendo colunas desnecessárias
df_atendimentos_fato = df_atendimentos_fato.drop(columns=['nm_agente', 'agente', 'Departamento', 'nm_departamento',
                                                          'nk_cidade', 'nm_cidade', 'uf', 'ddd', 'data_inicio', 'data_fim',
                                                          'ddd_cidade', 'tempo_de_atendimento_horas', 'tempo_de_atendimento_minutos',
                                                          'tempo_de_atendimento_segundos', 'tempo_de_atendimento_humano_horas',
                                                          'tempo_de_atendimento_humano_minutos', 'tempo_de_atendimento_humano_segundos',
                                                          'tempo_em_fila_horas', 'tempo_em_fila_minutos', 'tempo_em_fila_segundos',
                                                          'tempo_em_espera_horas', 'tempo_em_espera_minutos', 'tempo_em_espera_segundos'])

# Reordenando as colunas
orderm_columns = ['sk_agente', 'sk_departamento', 'sk_cidade', 'associacao',
                  'protocolo', 'telefone', 'data_criacao', 'hora_criacao',
                  'data_finalizacao', 'hora_finalizacao', 'data_transferencia',
                  'hora_transferencia', 'data_ultima_msg_agente',
                  'hora_ultima_msg_agente', 'data_ultima_msg_cliente',
                  'hora_ultima_msg_cliente', 'tempo_de_atendimentos',
                  'tempo_de_atendimento_humano', 'tempo_atendimento_automatizado',
                  'tempo_em_fila', 'tempo_de_espera']
df_atendimentos_fato = df_atendimentos_fato.reindex(columns=orderm_columns)

# Inserir dados na fato atendimentos no banco de dados
df_atendimentos_fato.to_sql(
    'ft_atendimentos', engine_dw, if_exists='append', index=False)
