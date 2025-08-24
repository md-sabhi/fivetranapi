from config_helper import config

from facilities_helper import facilities_helper
import fivetranapi
import colorama
from colorama import Fore, Back, Style
import time
import random
from logger import _logger
from fivetranapi import connection  # Import the Connection class if available


api_key = 'fivetran_api_key'
api_secret = 'fivetran_api_secret'

# cfg = config('fivetran_sql_server_payload.json')
# group_id = next((item['group_id'] for item in cfg['payload'] if 'group_id' in item), 'default_group_id')
try:
    ft = fivetranapi.connect(api_key, api_secret)
    response = ft.get_connections()
    
    if response is not None and len(response) > 0:
        # test: connection=response[0]
        # first_connection: connection = response[0]  # Assuming response[0] is already a Connection object
        
        # Filter connections where connected_by matches a specific value
        connected_by_value = "pug_persist"  # Replace with the value you want to match
        filtered_connections = [
            connection for c in response if connection.connected_by == connected_by_value
        ]
        
        # # Print the filtered connections
        # for connection in filtered_connections:
        #     _logger.info(f"Connection ID: {connection.id}")
        #     _logger.info(f"Connection Name: {connection.name}")
        #     _logger.info(f"Connected By: {connection.connected_by}")
    else:
        _logger.info("No response or empty response")
except Exception as e:
    _logger.error(e)
        

