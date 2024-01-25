from sqlalchemy import create_engine
import pyodbc

server = 'localhost:1433'
dbname = 'teste Growth Solutions'
username = 'userbi'
password = 'k200mh360'

DB = {'servername': 'localhost\SQLExpress',
      'database': 'teste Growth Solutions'}

#criando a conexão
conn = pyodbc.connect('DRIVER={SQL Server}; SERVER= ' +
                      DB['servername'] + ';DATABASE= ' + DB['database'] + 
                      ';Trusted_Connection=yes')
df = pd.read_sql_query('SELECT * FROM pip ')