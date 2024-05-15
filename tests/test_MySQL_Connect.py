import os
import sys
import re
#*''
from dotenv import load_dotenv
import pandas as pd

#**********
import pymysql
from mysql.connector.connection_cext import CMySQLConnection
import MySQLdb
import sqlalchemy
import jaydebeapi
#********

#Forsøk på  systematisk testing
from MySQL.MySQL_Connect import Connect

import unittest

class TestMySQLConnect(unittest.TestCase):
    
    def test_get_connection(self):
        print('Er nå inne i TestMySQLConnect.test_get_connection')
        subresults = []
        conn_pymysql = Connect().get_connection()
        subresults.append(isinstance(conn_pymysql,pymysql.connections.Connection))
        conn_pymysql.close()
        #
        conn_mysqlconnector = Connect(connection_type = "mysqlconnector").get_connection()
        subresults.append(isinstance(conn_mysqlconnector,CMySQLConnection))
        conn_mysqlconnector.close()
        #
        conn_mysqlclient = Connect(connection_type = "mysqlclient").get_connection()
        subresults.append(isinstance(conn_mysqlclient,MySQLdb.connections.Connection))
        conn_mysqlclient.close()
        #
        conn_jdbc = Connect(connection_type = "jdbc").get_connection()
        subresults.append(isinstance(conn_jdbc,jaydebeapi.Connection))
        conn_jdbc.close()

        # Check if every "sub-test" is passed 
        self.assertTrue(pd.Series(subresults).all())
        #
    #  

    
    def test_get_engine(self):
        print('Er nå inne i  TestOracleConnect.test_get_engine')
        conn_obj  = Connect()
        engine = conn_obj.get_engine()
        self.assertIsInstance(engine,sqlalchemy.engine.base.Engine)    
        engine.dispose() 
        
        
    