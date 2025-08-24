import pyodbc
from config_helper import config
from db_connection import DBConnection
import colorama
from colorama import Fore, Back, Style
from logger import _logger

global active_facilities

class facilities_helper:
    def __init__(self):
        self.logger = _logger('DEBUG', 'facilities_helper')
        self.logger.debug(f"initializing facilities_helper")

        
    def format_results(self, results):
        """Dictionary object of active facilities for the Azure Elastic Pool database.
        The dictionary object stores the key as a pattern string suffixed with the database name and 
        the value as the database name.
        The pattern string is 'cip_prd_sql_' and the database name is fetched from the database.
        """       
        formatted_results = {}
        for result in results:
            dbname = result.get('dbname')  # Replace 'dbname' with the actual column name from your result set
            key = f'cip_prd_sql_{dbname}'
            formatted_results[key] = result
        return formatted_results
       
    

        
    def fetch_active_facilities(self):
        """_summary_
        Azure Elastic Pool database names for active facilities.
        The function fetches the database names from the database and returns a dictionary object.
        Returns:
            _type_: dictionary object key being the pattern string 'cip_prd_sql_' suffixed with the database name.
            The value is the database name.
        """        
        cfg = config('configuration.json')
        query = """
                    SELECT
                    SUBSTRING(c.Value, 
                    CHARINDEX(';Initial Catalog=', c.Value)+1+16,
                    CHARINDEX(';User ID=', c.Value)- CHARINDEX(';Initial Catalog=', c.Value)-1-16
                    )dbname
                    from [Connection].[Connections] c
                    left join [Organization].[Plants] p on c.PlantId = p.Id
                    where p.isdeleted=0 and c.[Key] = 'SystemDatabase' order by dbname desc
                """  # Replace with your actual query and  c.Value like '%0000001578_System%'        
        with DBConnection(cfg) as db:
            self.logger.info('Connection to MS-SQL Server established successfully to fetch active Facilities database names')
            if db.connection:
                results = db.execute_query(query)
                self.logger.info('Query executed successfully :' + query)
                return self.format_results(results)
            else:
                self.logger.warning('Failed to establish connection to MS-SQL Server')
                return None
            
            
        
    def enable_cdc_on_all_cip_db_tables(self):
        """
        CDC enablement on database and tables for CIP Azure Elastic Pool database.
        The function is a wrapper for CIP Azure Elastic Pool database.
        The function iterates through the active facilities and enables CDC on each one with the role_name.        
        Disables existing CDC on the database, if any, and enables CDC on the database.
        The list of tables on which CDC needs to be enabled is fetched from the latest database.
        The function is designed to be used with caution, as it will remove all CDC tracking information.
        Ensure to use this only if you are sure that there is only one capture instance for the database.        
        """       
        global active_facilities
        if not active_facilities:
            self.logger.info('No Active Facilities Found in Database: ')
            return
        
        # get the latest base tables for cdc from the latest database, identified from the deployment database
        # new databases may not necessarily be enabled for the user, if tables are not visible for the latest db,
        # choose the one where you are able to see the tables in the database
        dbname = '0000001610_System'
        cfg = config('configuration.json')
        cfg.config["database"] = dbname
        with DBConnection(cfg) as dbconnxn:
            if dbconnxn.connection:
                self.logger.info(f"Connection to Latest deployment {dbname} established successfully to fetch tables for CDC Enablement.")
                latest_tables = self.get_latest_base_tables_for_cdc(dbconnxn)
                self.logger.info(f"Found {len(latest_tables)} tables for CDC enablement in database {dbname}.")
        
        for key, facility in active_facilities.items():
            dbname = facility.get('dbname')
            if not dbname:
                self.logger.info(f"Skipping database {dbname}. No database name found for facility {key}. Skipping.")
                continue
            if dbname != '0000000075_System':
                self.logger.info(f"Skipping database {dbname}.")
                continue

            cfg = config('configuration.json')
            cfg.config["database"] = dbname

            with DBConnection(cfg) as dbconnxn:
                if dbconnxn.connection:
                    self.logger.info(f"Connection to {dbname} established successfully.")
                    # disable CDC on database, if CDC already exists, else errors out 
                    # when enabling CDC when it is already enabled
                    self.disable_cdc_on_database(dbconnxn, dbname)
                    # enable CDC on database  
                    self.enable_cdc_on_database(dbconnxn, dbname)
                    # iterate through the latest tables and enable CDC on each one with the role_name
                    for table_schema, table_name in latest_tables:                    
                        self.enable_cdc_on_table(dbconnxn, table_schema, table_name)
                else:
                    self.logger.error(f"Failed to establish connection to {dbname}.")

        
    def enable_cdc_on_database(self, dbconnxn: DBConnection, dbname):
        """_summary_
        Enable CDC on the database
        This function enables Change Data Capture (CDC) on the specified database.
        Requires an existing database connection on which CDC needs to be enabled.
        If one needs to set up CDC on multiple databases, iterating over the function
        with database connection object and database name is recommended.
        Args:
            dbconnxn (DBConnection): The database connection object.
            dbname (str): The name of the database to enable CDC on. 
        Returns:
            None
        Raises:
            pyodbc.Error: If there is an error enabling CDC on the database.       
        """        
        try:
            # Explicitly set the database context
            use_db_query = f"USE [{dbname}]"
            dbconnxn.execute_query(use_db_query)
            
            # Enable CDC
            enable_cdc_query = "EXEC sys.sp_cdc_enable_db"
            dbconnxn.execute_query(enable_cdc_query)
            self.logger.info(f"CDC query {enable_cdc_query} executed for database {dbname}.")
            
            # Commit the transaction if necessary
            dbconnxn.connection.commit()
            
            # Verify if CDC is enabled
            verify_cdc_enabled_query = "SELECT is_cdc_enabled FROM sys.databases WHERE name = ?"
            results = dbconnxn.execute_query_with_params(verify_cdc_enabled_query, [dbname])
            if results and results[0]['is_cdc_enabled'] == 1:
                self.logger.info("{query} CDC ACTIVE, but no results to fetch.")
            else:
                self.logger.error(f"Failed to enable CDC for database {dbname}.")
        except pyodbc.Error as e:
            self.logger.error(f"Error enabling CDC for database {dbname}: {e}")
    
                
    def enable_cdc_on_db_by_db_name(self, dbname):
        """Enable CDC on the database
        This function enables Change Data Capture (CDC) on the specified database.
        The function establishes a database connection and executes the necessary SQL command to enable CDC.
        Invoke independently by providing dbname to enable CDC on the database.
        To set up CDC on tables, the database must be enabled for CDC first.
        Args:
            dbname (str): The name of the database to enable CDC on.
        """
        try:                       
            cfg = config('configuration.json')
            cfg.config["database"] = dbname

            with DBConnection(cfg) as dbconnxn:
                if dbconnxn.connection:
                    self.logger.info(f"Connection to {dbname} established successfully.")
                    self.enable_cdc_on_database(dbconnxn, dbname)                   
                else:
                    self.logger.error(f"Failed to establish connection to {dbname}.")
           
        except Exception as e:
            self.logger.error(f"Error disabling CDC on all tables in database {dbname}: {e}")

    def get_latest_base_tables_for_cdc(self, dbconnxn: DBConnection):
        """
        get the latest base tables for cdc from the latest Azure Elastic Pool Database, identified from the deployment database
        new databases may not necessarily be enabled for the user, if tables are not visible for the latest db,
        choose the one where you are able to see the tables in the database
        Args:
            dbconnxn (DBConnection): database connection of the latest database

        Returns:
            _type_: _description_
        """        
        query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'  and TABLE_SCHEMA <>  'cdc' and TABLE_SCHEMA <> 'sys' and  TABLE_SCHEMA <> 'dbo' order by 1,2"
        results = dbconnxn.execute_query(query)
        return [(row['table_schema'], row['table_name']) for row in results]

    # def enable_cdc_on_tables(self, dbconnxn: DBConnection, dbname, tables):
    #     try:
    #         for table_schema, table_name in tables:
    #             # Enable CDC on the table
    #             query = f"EXEC sys.sp_cdc_enable_table @source_schema = '{table_schema}' , @source_name  = '{table_name}' , @role_name   = role_name;"
    #             dbconnxn.execute_query(query)
                
    #             # Commit the transaction if necessary
    #             dbconnxn.connection.commit()
    #             self.logger.info("{query} executed successfully CDC enabled on {dbname}.{table_schema}.{table_name}, but no results to fetch.")
                
    #             # Verify if CDC is enabled on the table
    #             verify_query = "SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = ?"
    #             results = dbconnxn.execute_query_with_params(verify_query, [table_name])
    #             if results and results[0]['is_tracked_by_cdc'] == 1:
    #                 self.logger.info(f"CDC is successfully enabled for table {table_name}.")
    #             else:
    #                 self.logger.error(f"Failed to enable CDC for table {table_name}.")
    #     except pyodbc.Error as e:
    #         self.logger.error(f"Error enabling CDC for table: {table_name} in database {dbname}: {e}")
    
    def enable_cdc_on_table(self, dbconnxn: DBConnection, table_schema, table_name, role_name='role_name'):
        try:
            # Enable CDC on the table
            query = f"EXEC sys.sp_cdc_enable_table @source_schema = '{table_schema}' , @source_name  = '{table_name}' , @role_name   = {role_name};"
            dbconnxn.execute_query(query)
            
            # Commit the transaction if necessary
            dbconnxn.connection.commit()
            self.logger.info(f"{query} executed successfully CDC enabled on {table_schema}.{table_name}, but no results to fetch.")
            
            # Verify if CDC is enabled on the table
            verify_query = "SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = ?"
            results = dbconnxn.execute_query_with_params(verify_query, [table_name])
            if results and results[0]['is_tracked_by_cdc'] == 1:
                self.logger.info(f"CDC is successfully enabled for table {table_schema}.{table_name} with role_name {role_name}.")
            else:
                self.logger.error(f"Failed to enable CDC for table {table_schema}.{table_name} with role_name {role_name}.")            
                
        except pyodbc.Error as e:
            self.logger.error(f"Error enabling CDC for schema:  {table_schema}.{table_name} with role_name {role_name}: {e}")
       
    def enable_cdc_on_table_by_schema_table(self, dbname, schema, table, role_name):
        """_summary_
        Modifying the existing capture instance independently for a table in a database schema
        Invoked independently for a selected database schema table
        Establishing connection to the database from configuration.json
        Args:
            dbname (_type_): name of the database, establishing connection
            schema (_type_): name of the schema 
            table (_type_): naem of the table
        """        
        try:
            cfg = config('configuration.json')
            cfg.config["database"] = dbname

            with DBConnection(cfg) as dbconnxn:
                if dbconnxn.connection:
                    self.logger.info(f"Connection to {dbname} established successfully.")
                    self.enable_cdc_on_table(dbconnxn, schema, table, role_name)                   
                else:
                    self.logger.error(f"Failed to establish connection to {dbname}.")
        except Exception as e:
            self.logger.error(f"Error disabling CDC on all tables in database {dbname}: {e}")     
            
    def disable_cdc_on_database(self, dbconnxn:DBConnection, dbname):
        """ Disable CDC on the database
        This function disables Change Data Capture (CDC) on the specified database.
        If there is a capture instance for the database, it will be disabled as well.
        It also verifies if CDC is disabled successfully.
        Use this function with caution, as it will remove all CDC tracking information.
        Use this function only if you are sure that there is only one capture instance for the database, with 
        multiple capture instances, disabling CDC will impact removal of other capture instances.
        Query to disable current CDC instance on the database: 
        EXEC sys.sp_cdc_disable_db;            
        Args:
            dbconnxn (DBConnection): The database connection object.
            dbname (str): The name of the database to disable CDC on.       
        """
        try:
            disable_cdc_query = "EXEC sys.sp_cdc_disable_db"
            dbconnxn.execute_query(disable_cdc_query)
            self.logger.info(f"CDC disabled command executed for database {dbname}.")
            
            # Commit the transaction if necessary
            dbconnxn.connection.commit()
            
            # Verify if CDC is disabled
            verify_query = "SELECT is_cdc_enabled FROM sys.databases WHERE name = ?"
            results = dbconnxn.execute_query_with_params(verify_query, [dbname])
            if results and results[0]['is_cdc_enabled'] == 0:
                self.logger.info(f"CDC is successfully disabled for database {dbname}.")
            else:
                self.logger.error(f"Failed to disable CDC for database {dbname}.")
        except pyodbc.Error as e:
            self.logger.error(f"Error disabling CDC for database {dbname}: {e}")
    
    
    
    def disable_cdc_on_db_by_db_name(self,dbname):
        """Wrapper function to disable the CDC on the database.
        This function establishes a database connection and executes the necessary SQL command to disable CDC.
        Invoke independently by providing dbname to disable CDC on the database.
        Establishing connection to the database from configuration.json
        Args:
            dbname (_type_): the databasename name where CDC is to be disabled
        """        
        try:                          
            cfg = config('configuration.json')
            cfg.config["database"] = dbname

            with DBConnection(cfg) as dbconnxn:
                if dbconnxn.connection:
                    self.logger.info(f"Connection to {dbname} established successfully.")
                    self.disable_cdc_on_database(dbconnxn, dbname)                   
                else:
                    self.logger.error(f"Failed to establish connection to {dbname}.")
           
        except Exception as e:
            self.logger.error(f"Error disabling CDC on all tables in database {dbname}: {e}")
            
    def disable_cdc_on_table(self, dbconnxn:DBConnection, schema, table, capture_instance):
        """_summary_
        Query to disable current CDC instance: 
        EXEC sys.sp_cdc_disable_table
            @source_schema = [<schema>],
            @source_name   = [<table>],
            @capture_instance   = [<capture_instance>];
        Args:
            dbconnxn (DBConnection): _description_
            dbname (_type_): _description_
            table (_type_): _description_
        """        
        try:
            # Disable CDC on the table
            query = f"EXEC sys.sp_cdc_disable_table @source_schema = '{schema}' , @source_name  = '{table}', @capture_instance = '{capture_instance}'"
            dbconnxn.execute_query(query)
            
            # Commit the transaction if necessary
            dbconnxn.connection.commit()
            
            # Verify if CDC is disabled on the table
            verify_query = "SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = ?"
            results = dbconnxn.execute_query_with_params(verify_query, [table])
            if results and results[0]['is_tracked_by_cdc'] == 0:
                self.logger.info(f"CDC is successfully disabled for table {table}.")
            else:
                self.logger.error(f"Failed to disable CDC for table {table}.")
        except pyodbc.Error as e:
            self.logger.error(f"Error disabling CDC for table: {table} : {e}")
            
    def disable_cdc_on_table_by_schema_table(self, dbname, schema, table, capture_instance):
        """_summary_
        Modifying the existing capture instance independently for a table in a database schema
        Invoked independently for a selected database schema table
        Establishing connection to the database from configuration.json
        Args:
            dbname (_type_): name of the database, establishing connection
            schema (_type_): name of the schema 
            table (_type_): naem of the table
        """        
        try:
            cfg = config('configuration.json')
            cfg.config["database"] = dbname

            with DBConnection(cfg) as dbconnxn:
                if dbconnxn.connection:
                    self.logger.info(f"Connection to {dbname} established successfully.")
                    self.disable_cdc_on_table(dbconnxn, schema, table, capture_instance)                   
                else:
                    self.logger.error(f"Failed to establish connection to {dbname}.")
        except Exception as e:
            self.logger.error(f"Error disabling CDC on all tables in database {dbname}: {e}")
            
    
    
    def modify_existing_capture_instance_on_all_cip_tables(self):
        """_summary_ TODO: drive from capture instance, rather than latest base tables
        Existing capture instance modified 
        Scenario: when new coluns have been added to tables on an existing capture instance
        Select capture instance (i.e. column capture_instance) name for schema and table for the role name (i.e., Fivetran user) provided: 
        EXEC sys.sp_cdc_help_change_data_capture @source_schema = '<schema>', @source_name = 'test_table';
        Query to disable current CDC instance: 
        EXEC sys.sp_cdc_disable_table
            @source_schema = [<schema>],
            @source_name   = [<table>],
            @capture_instance   = [<capture_instance>];
        Query to create a new CDC instance: 
         EXEC sys.sp_cdc_enable_table
            @source_schema = [<schema>],
            @source_name   = [<table>],
            @role_name     = [<username>],
            @supports_net_changes = 0;
        Args:
            dbname (_type_): name of the database
            schema (_type_): name of the schema 
            table (_type_): naem of the table
        """        
        try:
            global active_facilities
            if not active_facilities:
                self.logger.info('No Active Facilities Found in Database: ')
                return
        
            # get the latest base tables for cdc from the latest database, identified from the deployment database
            # new databases may not necessarily be enabled for the user, if tables are not visible for the latest db,
            # choose the one where you are able to see the tables in the database
            dbname = '0000000039_system'
            cfg = config('configuration.json')
            cfg.config["database"] = dbname
            with DBConnection(cfg) as dbconnxn:
                if dbconnxn.connection:
                    self.logger.info(f"Connection to Latest deployment {dbname} established successfully to fetch tables for CDC Enablement.")
                    latest_tables = self.get_latest_base_tables_for_cdc(dbconnxn)
                    self.logger.info(f"Found {len(latest_tables)} tables for CDC enablement in database {dbname}.")
            
            for key, facility in active_facilities.items():
                dbname = facility.get('dbname')
                if not dbname:
                    self.logger.info(f"Skipping database {dbname}. No database name found for facility {key}. Skipping.")
                    continue
                if dbname != '0000000039_system':
                    self.logger.info(f"Skipping database {dbname}.")
                    continue

                cfg = config('configuration.json')
                cfg.config["database"] = dbname

                with DBConnection(cfg) as dbconnxn:
                    if dbconnxn.connection:
                        self.logger.info(f"Connection to {dbname} established successfully.")                      
                        # iterate through the latest tables and enable CDC on each one with the role_name
                        for table_schema, table_name in latest_tables:                    
                            self.disable_cdc_on_table(dbconnxn, table_schema, table_name)
                            self.enable_cdc_on_table(dbconnxn, table_schema, table_name)
                    else:
                        self.logger.error(f"Failed to establish connection to {dbname}.")

        except Exception as e:
            self.logger.error(f"Error disabling CDC on all tables in database {dbname}: {e}")
        
    


# Example usage
if __name__ == "__main__":
    try:
        fh = facilities_helper()
        active_facilities = fh.fetch_active_facilities()
        if active_facilities:
            fh.logger.info(f"Total Active Databases: {len(active_facilities)}")
            fh.logger.newline()
            #fh.enable_cdc_on_all_cip_db_tables() # tested and works, iterates through each db all tables and executes
            # fh.logger.debug(active_facilities)
            #fh.disable_cdc_on_db_by_db_name('0000001610_System') # tested and works
            #fh.enable_cdc_on_db_by_db_name('0000001610_System') # tested and works
            fh.disable_cdc_on_table_by_schema_table('0000000021_system', 'ProductionData', 'MembraneProductionRuns','ProductionData_MembraneProductionRuns') # tested and works
            fh.enable_cdc_on_table_by_schema_table('0000000021_system', 'ProductionData', 'MembraneProductionRuns','ProductionData_MembraneProductionRuns') # tested and works
            fh.disable_cdc_on_table_by_schema_table('0000000021_system', 'ProductionData', 'Notifications','ProductionData_Notifications') # tested and works
            fh.enable_cdc_on_table_by_schema_table('0000000021_system', 'ProductionData', 'Notifications','ProductionData_Notifications') # tested and works
        else:
            fh.logger.error("No active facilities found.")
    except Exception as e:
        fh.logger.error(f"Error: {e}")
        
        
        # Error: New Columns found in CDC Instance. Performing a FULL REFRESH does not help, need to disable and enable the CDC instance and then perform a full refresh.