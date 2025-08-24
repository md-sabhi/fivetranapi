import re
from config_helper import config
from facilities_helper import facilities_helper
import fivetranapi
import colorama
from colorama import Fore, Back, Style
import time
import random
from logger import _logger
from readfiles_container import AzureStorageReader

class FivetranUtils:    
    def __init__(self):
        self.logger = _logger('DEBUG', 'FivetranUtils')
        self.api_key = 'key'
        self.api_secret = 'secret'
        self.ftapiconnxn = fivetranapi.connect(self.api_key, self.api_secret)

    # function to create fivetran connectors for all facilities
    # replaces the values of the database and schema_prefix in the config_fivetran_payload.json file
    # and creates a connector for each facility
    def cip_facilities_create_connectors(self, cip_active_facilities):
        try:
            
            active_facilities = cip_active_facilities
            if active_facilities:
                self.logger.info(f"Total Active Databases: {len(active_facilities)}")
                self.logger.info(active_facilities)
                self.logger.info("Exiting Code")
            else:
                self.logger.error("No active facilities found.")
        

            counter = 0
            for key, facility in active_facilities.items():
                
                dbname = facility.get('dbname')
                time.sleep(random.uniform(2, 3))
                if not dbname:
                    self.logger.info(f"Skipping database {dbname}. No database name found for facility {key}. Skipping.")
                    continue

                self.create_connector_for_database(dbname, key.lower(), counter)
        except Exception as e:
            self.logger.error(f"Error: {e}")
    
    
    def disable_enable_connectors_by_destination(self, group_id, status):
        try:
            response = self.ftapiconnxn.get_connections_of_group(group_id)
            if response is not None and len(response) > 0:
                for conn in response:
                    t: fivetranapi.connection  = conn
                    self.enable_disable_connector(t.schema, t.id, {'paused': 'True'})
            else:
                self.logger.info("No response or empty response")
        except Exception as e:
            self.logger.error(e)
            
    def delete_connectors_by_destination(self, group_id):
        try:
            response = self.ftapiconnxn.get_connections_of_group(group_id)
            if response is not None and len(response) > 0:
                for conn in response:
                    t: fivetranapi.connection  = conn
                    self.delete_connector(t.id)
            else:
                self.logger.info("No response or empty response")
        except Exception as e:
            self.logger.error(e)
            
    # function to create fivetran connectors for all facilities
    # replaces the values of the database and schema_prefix in the config_fivetran_payload.json file
    # and creates a connector for each facility
    def blobs_create_connectors(self,files, config_file):
        try:
            if files:
                self.logger.info(f"Total Active Files: {len(files)}")         
            else:
                self.logger.error("No Files found.")
            counter = 0
            for file in files:
                time.sleep(random.uniform(2, 3))
                file['connector_id'] = self.create_connector_for_blob(file['table_name'].lower(), file['file_name'], config_file,counter)
                self.resync_connetor(file['connector_id'])
        except Exception as e:
            self.logger.error(f"Error: {e}")



    # function to create a single fivetran azure blob connector overriding the values present in the config_fivetran_payload.json
    def create_connector_for_blob(self, table_name, file_name, config_file,counter):
        try:
            cfg = config(config_file)      
            cfg.config["config"]["table"] = table_name
            cfg.config["config"]["pattern"] = file_name
            payload = cfg.config
            self.logger.info(f"Payload: {payload}")
            response = self.ftapiconnxn.create_connector(payload)
            if response is not None:
                counter += 1
                self.logger.info(f"{counter}). {table_name} Connector created successfully for database {file_name}.")
                return response['data']['id']
        except Exception as e:
            self.logger.error(f"{counter}). {table_name} Connector setup failure for database {file_name}.")
            self.logger.error(e)
            
    def blobs_to_connectors_compare(self, files_in_container):
        try:
            files_wo_container=[]
            if files_in_container:
                self.logger.info(f"Total Active Files: {len(files_in_container)}")         
            else:
                self.logger.error("No Files found.")
            counter = 0
            try:
                # response = self.ftapiconnxn.get_connections_of_group('neatness_whoops') #SRC_DEV_NSAP_DB
                response = self.ftapiconnxn.get_connections_of_group('state_exceptional') #SRC_TST_NSAP_DB
                if response is not None and len(response) > 0:
                    # TODO: put up the logic to setup comparision
                    print()
                else:
                    self.logger.info("No response or empty response")                
                return files_wo_container
            except Exception as e:
                self.logger.error(e)
            
            
                
        except Exception as e:
            self.logger.error(f"Error: {e}")
    
    # function to create a single fivetran azure sql db connector 
    # function sets up the connector by reading the values present in the config_fivetran_payload.json
    # ensure to have the right values for the database and schema_prefix in the config_fivetran_payload.json
    def create_connector_for_database(self, database, schema, counter):
        try:
            cfg = config('config_fivetran_payload.json')
            cfg.config["config"]["database"] = database
            cfg.config["config"]["schema_prefix"] = schema.lower()

            payload = cfg.config
            self.logger.info(f"Payload: {payload}")
            response = self.ftapiconnxn.create_connector(payload)
            if response is not None:
                counter += 1
                self.logger.info(f"{counter}). {schema} Connector created successfully for database {database}.")
        except Exception as e:
            self.logger.error(f"{counter}). {schema} Connector setup failure for database {database}.")
            self.logger.error(e)
            
    # enable or disable a fivetran connector
    def enable_disable_connector(self,connector_name, connector_id, params):
        try:
            payload = params
            response = self.ftapiconnxn.update_connector(connector_id, payload)
            if response is not None:
                self.logger.info(f"Connector {connector_name} with {connector_id} Paused State: {payload['paused']} successfully.")
        except Exception as e:
            self.logger.error(f"Failed to pause connector Connector {connector_name} with {connector_id}.")
            self.logger.error(e)
            
    def delete_connector(self, connector_id):
        try:
            response = self.ftapiconnxn.delete_connector(connector_id)
            if response is not None:
                self.logger.info(f"Connector {connector_id} deleted successfully.")
        except Exception as e:
            self.logger.error(f"Failed to delete connector {connector_id}.")
            self.logger.error(e)
            
    def resync_connetor(self, connector_id):
        try:
            response = self.ftapiconnxn.resync_connector(connector_id)
            if response is not None:
                self.logger.info(f"Connector {connector_id} for resync successfully.")
        except Exception as e:
            self.logger.error(f"Failed to resync connector {connector_id}.")
            self.logger.error(e)
            
    
            


if __name__ == "__main__":
    connectors = FivetranUtils()
    # create fivetran connectors for all facilities
    # connectors.cip_facilities_create_connectors()
    # create a single fivetran db connector
    #connectors.create_connector_for_database("0000001610_System", "cip_prd_sql_0000001610_System", 1)
    # pause a single fivetran connector
    
    # files = [
    #     # {'table_name': 'CRM_CNCCRMPRCUS876', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_CNCCRMPRCUS876_MD.tsv'},
    #     # {'table_name': 'CRM_CRMC_PROC_TYPE_T', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_CRMC_PROC_TYPE_T_MD.tsv'},
    #     # {'table_name': 'CRM_ZBASUBTYPE_T', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_ZBASUBTYPE_T_MD.tsv'},
    #     # {'table_name': 'CRM_ZTSD_GEOREGION_T', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_ZTSD_GEOREGION_T_MD.tsv'},
    #     # {'table_name': 'ECC_A670', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A670_TD.tsv'},
    #     # {'table_name': 'ECC_A671', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A671.tsv'},
    #     # {'table_name': 'ECC_A671_TD', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A671_TD.tsv'},
    #     # {'table_name': 'ECC_KONP_TD', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_KONP_TD.tsv'},
    #     # {'table_name': 'ECC_T178T', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_T178T_MD.tsv'},
    #     # {'table_name': 'ECC_TVV1T', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_TVV1T_MD.tsv'},
    #     # {'table_name': 'ECC_TVV3T', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_TVV3T_MD.tsv'},
    #     {'table_name': 'ECC_T179T', 'file_name':'FL_DGTLSC_NLC_ECC_FILE_T179T_MD.tsv'}
    # ]
    
    files = [
        # {'table_name': 'NLC_CRM_CNCCRMPRCUS876', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_CNCCRMPRCUS876_MD.tsv'},
        # {'table_name': 'NLC_CRM_CRMC_PROC_TYPE_T', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_CRMC_PROC_TYPE_T_MD.tsv'},
        # {'table_name': 'NLC_CRM_ZBASUBTYPE_T', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_ZBASUBTYPE_T_MD.tsv'},
        {'table_name': 'NLC_CRM_ZTSD_EPC_SORG', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_ZTSD_EPC_SORG_MD.tsv'},
        # {'table_name': 'NLC_CRM_ZTSD_GEOREGION_T', 'file_name':'FL_GSCSCM_NLC_CRM_FILE_ZTSD_GEOREGION_T_MD.tsv'},
        {'table_name': 'NLC_ECC_A581', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A581_TD.tsv'},
        
        # {'table_name': 'NLC_ECC_A670', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A670_TD.tsv'},
        # {'table_name': 'NLC_ECC_A671', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A671_TD.tsv'},
        # {'table_name': 'NLC_ECC_A672', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A672_TD.tsv'},
        # {'table_name': 'NLC_ECC_A673', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A673_TD.tsv'},
        # {'table_name': 'NLC_ECC_A691', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A691_TD.tsv'},
        # {'table_name': 'NLC_ECC_A823', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A823_TD.tsv'},
        {'table_name': 'NLC_ECC_A873', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_A873_TD.tsv'}
        # {'table_name': 'NLC_ECC_KONP', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_KONP_TD.tsv'},
        # {'table_name': 'NLC_ECC_T178T', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_T178T_MD.tsv'},
        # {'table_name': 'NLC_ECC_TVV1T', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_TVV1T_MD.tsv'},
        # {'table_name': 'NLC_ECC_TVV3T', 'file_name':'FL_GSCSCM_NLC_ECC_FILE_TVV3T_MD.tsv'}

        
    ]
    # connectors.blobs_create_connectors(files)
    connectors.blobs_create_connectors(files, 'config_fivetran_blob_qa_payload.json')
    
    # enab
    # connectors.enable_disable_connector("previously_cheer","")
    
     # diasble all connectors of a destination
    #connectors.disable_enable_connectors_by_destination('regenerated_matched',"False")    
    
    # fh = facilities_helper() 
    # cip_active_sites = fh.fetch_active_facilities()
    
    # create connectors
    #connectors.facilities_create_connectors(cip_active_sites)
    
    # enable all cip connectors
    
    # delete all connectors of a destination
    #connectors.delete_connectors_by_destination('regenerated_matched') 1610
    
    # resync a particular connector
    # connectors.resync_connetor("dream_moneywise")
    # connectors.resync_connetor("undercut_recreation")
    
    # connection_string = ""
    # container_name = "ib-sap"
    # folder_name = "NSAP/PricingFivetran"
    # output_file = "PricingFivetran.txt"

    # reader = AzureStorageReader(connection_string, container_name, folder_name, output_file)
    # reader.connect()
    # files_in_container = reader.list_files_in_folder()
    
    # get_files_wo_connector = connectors.blobs_to_connectors_compare(files_in_container)
    
    # print(f"Files without connectors: {get_files_wo_connector}")


