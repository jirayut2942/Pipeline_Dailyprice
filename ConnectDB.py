import pymysql
from sqlalchemy import create_engine

# database
# armpc
user = 'root'
passw = 'wE1dhomxef'
host =  'localhost'
port = 3306
database = 'stocks_db'

engine = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database , echo=False)