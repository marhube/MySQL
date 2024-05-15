#************** Start importing python modules for testing and degbugging
import sys # For sys.exit()
import time
import os # For os.listdir()
#*********** End importing python modules for testing and degbugging
import pandas as pd
import re
#********* Start  MySQL-related imports
import mysql.connector
import pymysql
import MySQLdb
import MySQLdb.connections
from mysql.connector.connection_cext import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection
import sqlalchemy
import jaydebeapi # type: ignore
#********** End  MySQL-related imports

#*********  Start passordelatert
from dotenv import load_dotenv
from decouple import config
#*********  Slutt databaserelatert



#essage)
# Definerer min egen "exception" slik at bare trenger å definere "exception-beskjeden" en gang
class MySQLError(Exception):
    #Custom exception class
    def __init__(self, message):
        super().__init__(message)
                         
def raise_unsupported_connection_type() -> None:
    raise MySQLError("Connection type has to be one of 'mysql connector','pymysql','mysqlclient','sqlalchemy' or 'jdbc' ") 


class Connect:
    def __init__(
        self,
        user = 'mhunting',
        db_name = 'test_db',
        connection_type = "pymysql", 
        jdbc_driver_path = "/home/m01315/MySQL/Jars/mysql-connector-j-8.3.0/mysql-connector-j-8.3.0.jar"
        ):
        if connection_type not in ["mysqlconnector","pymysql","mysqlclient","sqlalchemy","jdbc"]:
            raise_unsupported_connection_type()
        #
        self.user = user
        self.db_name = db_name
        self.connection_type  = connection_type 
        self.jdbc_driver_path = jdbc_driver_path

    # Connect to the database
        
    def get_mysqlconnector_connection(self) -> PooledMySQLConnection|MySQLConnectionAbstract:
        load_dotenv()
        host = config('MYSQL_HOST')
        port = config('MYSQL_PORT')
        password = config('_'.join(['MYSQL_PASSWORD',self.user]))
        config_dict = {
                'user': self.user,
                'password': password,
                'host': host,
                'database': self.db_name,
                'port': int(port)
                }
            #
        conn = mysql.connector.connect(**config_dict)
        return conn

    def get_pymysql_connection(self) -> PooledMySQLConnection|MySQLConnectionAbstract:
        load_dotenv()
        host = config('MYSQL_HOST')
        port = config('MYSQL_PORT')
        password = config('_'.join(['MYSQL_PASSWORD',self.user]))

        # .encode("utf-8") is a workaround to handle "non-ASCII" -characters
        conn = pymysql.connect(
            host = host,
            user = self.user,
            password = password.encode('utf-8'),     #type: ignore 
            database = str(self.db_name),
            port = int(port),
            charset='utf8mb4'
            )
        #

        return conn

    
    def get_mysqlclient_connection(self) -> PooledMySQLConnection|MySQLConnectionAbstract:
        load_dotenv()
        host = config('MYSQL_HOST')
        port = config('MYSQL_PORT')
        password = config('_'.join(['MYSQL_PASSWORD',self.user]))
        conn = MySQLdb.connect(
            host = host,
            user = self.user,
            passwd = password,
            db = self.db_name,
            port = int(port)
            )
        #

        return conn
    
    def get_jdbc_connection(self):
        load_dotenv()
        driver = config('MYSQL_DRIVER')
        password = config('_'.join(['MYSQL_PASSWORD',self.user]))
        conn_string = self.create_mysql_connection_string()
        #
        conn = jaydebeapi.connect(
            jclassname=driver,
            url=self.replace_env_variables(conn_string),
            driver_args=[self.user, password],
            jars=self.jdbc_driver_path,
            libs=None
            )
        #

        return conn

    def get_connection(self) -> (
            PooledMySQLConnection|
            MySQLConnectionAbstract|
            jaydebeapi.Connection
            ):        
        conn = None
        #
        if self.connection_type == "mysqlconnector":
            conn = self.get_mysqlconnector_connection()
        elif self.connection_type == "pymysql":
            conn = self.get_pymysql_connection()
        elif self.connection_type == "mysqlclient":
            conn = self.get_mysqlclient_connection()
        elif self.connection_type ==  "jdbc":
            # Start the JVM with the JDBC driver
            conn = self.get_jdbc_connection()
        else:
            raise_unsupported_connection_type()
        return conn

    
    def replace_env_variables(self,s: str) -> str:
        # This function will find all occurrences of ${VAR_NAME} in the string
        # and replace them with the value of the environment variable VAR_NAME.
        # Define a pattern to match ${ANYTHING_HERE}
        load_dotenv()
        pattern = re.compile(r'\$\{(.+?)\}')
        # Replace each found pattern with the corresponding environment variable
        def replace(match):
            var_name = match.group(1)
            #Passord er avhengig av brukernavn og derfor et spesialtilfelle
            if var_name == 'MYSQL_PASSWORD':
                var_name = '_'.join([var_name,self.user])
            #

            return os.environ.get(var_name, '')  # Replace with env variable value
        #
        return pattern.sub(replace, s)

    def create_mysql_connection_string(self) -> str:
        conn_string = ''
        if self.connection_type in ['mysqlconnector',"pymysql",'mysqlclient']:
            driver_name = ""
            if self.connection_type in ['mysqlconnector',"pymysql"]:
                driver_name = self.connection_type 
            elif self.connection_type == "mysqlclient":
                driver_name = "mysqldb"
            #
            conn_string = f"mysql+{driver_name}://{self.user}:${{MYSQL_PASSWORD}}@${{MYSQL_HOST}}:${{MYSQL_PORT}}/{self.db_name}"
        elif self.connection_type == "jdbc":
            conn_string = f"jdbc:mysql://${{MYSQL_HOST}}:${{MYSQL_PORT}}/{self.db_name}"
        else:
            raise_unsupported_connection_type()
        return conn_string


    def create_jdbc_properties_dict(self) -> dict:
        properties = {
            "user": self.user,
            "password": "${MYSQL_PASSWORD}",
            "driver": "org.postgresql.Driver"
        }
        return properties            
 #
   
    def get_engine(self) -> sqlalchemy.engine.base.Engine:
        conn_string_implicit = self.create_mysql_connection_string()
        conn_string = self.replace_env_variables(conn_string_implicit)
        engine = sqlalchemy.create_engine(conn_string)
        return engine
        

   #
   
#

