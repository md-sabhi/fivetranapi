import json
import pyodbc
from config_helper import config
import colorama
from colorama import Fore, Back, Style
from logger import _logger

class DBConnection:
    logger = _logger('DEBUG', 'DBConnection')
    def __init__(self, cnfg: config):
        self.connection = None
        self.cursor = None
        self.config = cnfg.config
        

    def __enter__(self):
        self.setup_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup_connection()

    def setup_connection(self):
        #cfg = config('configuration.json')
        
        server = self.config['server']
        database = self.config['database']
        username = self.config['user']
        password = self.config['password']
        
        try:
            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            DBConnection.logger.info(f"Connection established successfully to database: {database}")
        except pyodbc.Error as e:
            DBConnection.logger.error(f"Error connecting to database: {e}")
            self.connection = None
            self.cursor = None

    def cleanup_connection(self):
        if self.cursor:
            self.cursor.close()
            DBConnection.logger.info("Cursor closed successfully.")
        if self.connection:
            self.connection.close()
            DBConnection.logger.info("Connection closed successfully.")

    def execute_query(self, query):
        if self.connection is None or self.cursor is None:
            DBConnection.logger.error("Unable to establish connection to database.")
            return []

        try:
            self.cursor.execute(query)
            if self.cursor.description is None:
                return []
            columns = [column[0] for column in self.cursor.description]
            results = []
            for row in self.cursor.fetchall():
                result = {columns[i]: row[i] for i in range(len(columns))}
                results.append(result)
            return results
        except pyodbc.Error as e:
            if "is already enabled" in str(e):
                DBConnection.logger.info("CDC is already enabled on this database.")
            else:
                DBConnection.logger.error(f"Error executing query: {e}")
            return []

    def execute_query_with_params(self, query, params):
        if self.connection is None or self.cursor is None:
            DBConnection.logger.error("Unable to establish connection to database.")
            return []

        try:
            self.cursor.execute(query, params)
            if self.cursor.description is None:
                return []
            columns = [column[0] for column in self.cursor.description]
            results = []
            for row in self.cursor.fetchall():
                result = {columns[i]: row[i] for i in range(len(columns))}
                results.append(result)
            return results
        except pyodbc.Error as e:
            DBConnection.logger.error(f"Error executing query: {e}")
            return []
# def execute_query_with_params(self, query, params):
    #     cursor = self.connection.cursor()
    #     cursor.execute(query, params)
    #     columns = [column[0] for column in cursor.description]
    #     results = []
    #     for row in cursor.fetchall():
    #         results.append(dict(zip(columns, row)))
    #     cursor.close()
    #     return results
