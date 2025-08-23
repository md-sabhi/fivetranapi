import logging
import requests
from requests.auth import HTTPBasicAuth

class _logger():
    def __init__(self, level, name=__file__):
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level))
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        self.logger = logger

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

class account():
    def __init__(self, account):
        self._json = account
        self.account_id = account['account_id']
        self.system_key_id = str(account['system_key_id'])
        self.account_name = account['account_name']
        self.user_id = account['user_id']

class connection():
    def __init__(self, connection):
        self._json = connection
        self.id = connection['id']
        self.service = connection['service']
        self.schema = connection['schema']
        self.paused = str(connection['paused'])
        self.status = connection['status']  
        self.config = str(connection['config'])
        self.daily_sync_time = str(connection['daily_sync_time'])
        self.succeeded_at = str(connection['succeeded_at'])
        self.sync_frequency = str(connection['sync_frequency'])
        self.group_id = connection['group_id']
        self.connected_by = connection['connected_by']
        self.setup_tests = str(connection['setup_tests'])
        if 'source_sync_details' in connection:
            self.source_sync_details = str(connection['source_sync_details'])
        else: 
            self.source_sync_details = str(None)
        self.service_version = str(connection['service_version'])
        self.created_at = connection['created_at']
        self.failed_at = str(connection['failed_at'])
        self.private_link_id = str(connection['private_link_id'])
        self.proxy_agent_id = str(connection['proxy_agent_id'])
        self.networking_method = str(connection['networking_method'])
        if 'connect_card' in connection:
            self.connect_card = str(connection['connect_card'])
        else:
            self.connect_card = str(None)
        self.pause_after_trial = str(connection['pause_after_trial'])
        self.data_delay_threshold = str(connection['data_delay_threshold'])
        self.data_delay_sensitivity = connection['data_delay_sensitivity']
        self.schedule_type = connection['schedule_type']
        if 'local_proccesing_agent_id' in connection:
            self.local_proccesing_agent_id = str(connection['local_proccesing_agent_id'])
        else:
            self.local_proccesing_agent_id = str(None)
        if 'connect_card_config' in connection:
            self.connect_card_config = str(connection['connect_card_config'])
        else:
            self.connect_card_config = str(None)
        if 'hybrid_deployment_agent_id' in connection:
            self.hybrid_deployment_agent_id = str(connection['hybrid_deployment_agent_id'])
        else:
            self.hybrid_deployment_agent_id = str(None)

class destination():
    def __init__(self, destination):
        self._json = destination
        self.id = destination['id']
        self.service = destination['service']
        self.region = destination['region']
        self.networking_method = str(destination['networking_method'])
        self.setup_status = destination['setup_status']
        self.daylight_saving_time_enabled = str(destination['daylight_saving_time_enabled'])
        self.private_link_id = str(destination['private_link_id'])
        self.group_id = destination['group_id']
        self.time_zone_offset = destination['time_zone_offset']
        if 'hybrid_deployment_agent_id' in destination:
            self.hybrid_deployment_agent_id = str(destination['hybrid_deployment_agent_id'])
        else:
            self.hybrid_deployment_agent_id = str(None)

class group():
    def __init__(self, group):
        self._json = group
        self.id = group['id']
        self.name = group['name']
        self.created_at = group['created_at']

class hybrid_deployment_agent():
    def __init__(self, hybrid_deployment_agent):
        self._json = hybrid_deployment_agent
        self.id = hybrid_deployment_agent['id']
        self.usage = str(hybrid_deployment_agent['usage'])
        self.registered_at = hybrid_deployment_agent['registered_at']
        self.display_name = hybrid_deployment_agent['display_name']
        self.group_id = hybrid_deployment_agent['group_id']

class privatelink():
    def __init__(self, privatelink):
        self._json = privatelink
        self.id = privatelink['id']
        self.name = privatelink['name']
        self.region = privatelink['region']
        self.service = privatelink['service']
        self.state = privatelink['state']
        self.account_id = privatelink['account_id']
        self.created_at = privatelink['created_at']
        self.created_by = privatelink['created_by']
        self.cloud_provider = privatelink['cloud_provider']
        self.state_summary = privatelink['state_summary']
        self.config = str(privatelink['config'])

class user():
    def __init__(self, user):
        self._json = user
        self.id = user['id']
        self.email = user['email']
        self.verified = str(user['verified'])
        self.invited = str(user['invited'])
        self.picture = str(user['picture'])
        self.phone = str(user['phone'])
        if 'role' in user:
            if user['role'] is None:
                self.role = str(None)
            else:
                self.role = user['role']
        else:
            self.role = str(None)
        self.active = str(user['active'])
        self.given_name = str(user['given_name'])
        self.created_at = user['created_at']
        self.family_name = str(user['family_name'])
        self.logged_in_at = str(user['logged_in_at'])

class connect():
    def __init__(self, api_key, api_secret, base_url='https://api.fivetran.com/v1'):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = base_url
        self.auth = HTTPBasicAuth(api_key, api_secret)
        self.logger = _logger('DEBUG', 'fivetran')

    def call_api(self, method, endpoint, payload=None): # {'limit': 100}
        url = f'{self.base_url}/{endpoint}'
        self.logger.debug(f"call_api using (method='{method}', endpoint='{url}', payload={str(payload)})")
        # handle cursor - should be part of h
        #    while "next_cursor" in response["data"]:
        #        print("paged")
        #        params = {"limit": limit, "cursor": response["data"]["next_cursor"]}
        #        url = "https://api.fivetran.com/v1/groups/{}/connectors".format(group_id)
        #        response_paged = requests.get(url=url, auth=a, params=params).json()
        #        if any(response_paged["data"]["items"]) == True:
        #            conn_list.extend(response_paged["data"]['items'])
        #    response = response_paged
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.api_key}:{self.api_secret}'
        }
        try:
            if method == 'GET':
                # only place we need to handle cursors for retrieval
                response = requests.get(url, headers=headers, auth=self.auth)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=payload, auth=self.auth)
            elif method == 'PATCH':
                response = requests.patch(url, headers=headers, json=payload, auth=self.auth)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, auth=self.auth)
            else:
                raise ValueError('Invalid request method.')
            if response is not None:
                response.raise_for_status()  # Raise exception for 4xx or 5xx responses
                # this is where we need to handle cursors
                return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f'Request failed: {e}')
            raise(e)
        
    def get_account_info(self):
        resp = self.call_api('GET', 'account/info')
        if resp is not None:
            return account(resp['data'])
        return None

    def print_account_info(self, account=None):
        if account is None:
            account = self.get_account_info()
        self.logger.info('Account Id: ' + account.account_id + ' Account Name: ' + account.account_name + ' User Id: ' + account.user_id + ' System Key Id: ' + account.system_key_id)

    def get_connections(self, limit=100):
        resp = self.call_api('GET', 'connections', {'limit': limit})
        if resp is not None:
            connections = []
            for item in resp['data']['items']:
                connections.append(connection(item))
            return connections
        return None

    def print_connections(self, connections=None):
        if connections is None:
            connections = self.get_connections()
        for connection in connections:
            self.logger.info('Connection Id: ' + connection.id + ' Group Id: ' + connection.group_id + ' Service: ' + connection.service + ' Schema: ' + connection.schema + ' Connected By: ' + connection.connected_by + ' Created At: ' + connection.created_at + ' Succeeded At: ' + connection.succeeded_at + ' Failed At: ' + connection.failed_at + ' Paused: ' + connection.paused + ' Pause After Trial: ' + connection.pause_after_trial + ' Sync Frequency: ' + connection.sync_frequency + ' Data Delay Threshold: ' + connection.data_delay_threshold + ' Data Delay Sensitivity: ' + connection.data_delay_sensitivity + ' Daily Sync Time: ' + connection.daily_sync_time + ' Schedule Type: ' + connection.schedule_type + ' Networking Method: ' + connection.networking_method + ' Proxy Agent Id: ' + connection.proxy_agent_id)

    def get_connectors(self):
        connectors = self.call_api('GET', 'metadata/connector-types')
        if connectors is not None:
            return connectors
        return

    def get_connector_schema(self, connector):
        schema = self.call_api('GET', f'metadata/connector-types/{connector}')
        if schema is not None:
            return schema
        return None

    def create_connector(self, payload):
        resp = self.call_api('POST', 'connectors', payload)
        # Should return a proper object representing the response of creating a connector
        if resp is not None:
            return resp
        return None
    
    def delete_connector(self, connector_id):
        resp = self.call_api('DELETE', 'connectors/' + connector_id)
        if resp is not None:
            return resp
        return None

    def copy_connector(self, connector_id):
        # implement
        return None

    def get_destinations(self):
        # this has potential for cursor
        dests = self.call_api('GET', 'destinations')
        if dests is not None:
            destinations = []
            for _destination in dests['data']['items']:
                destinations.append(destination(_destination))
            return destinations
        return None
    
    def get_destination_detail(self, destination_id):
        destination = self.call_api('GET', 'destinations/' + destination_id)
        if destination is not None:
            return destination(destination['data'])
        return None
    
    def print_destinations(self):
        destinations = self.get_destinations()
        if destinations is not None:
            for destination in destinations:
                self.logger.info('Destination Id: ' + destination.id + ' Group Id: ' + destination.group_id + ' Networking Method: ' + destination.networking_method + ' Service: ' + destination.service + ' Private Link Id: ' + destination.private_link_id + ' Region: ' + destination.region + ' Timezone Offset: ' + destination.time_zone_offset + ' Setup Status: ' + destination.setup_status + ' Daylight Saving Time Enabled: ' + destination.daylight_saving_time_enabled + ' Hybrid Deployment Agent Id: ' + destination.hybrid_deployment_agent_id)

    def get_groups(self):
        # this has potential for cursor
        resp = self.call_api('GET', 'groups')
        if resp is not None:
            groups = []
            for item in resp['data']['items']:
                groups.append(group(item))
            return groups
        return None
    
    def get_group_detail(self, group_id):
        group = self.call_api('GET', 'groups/' + group_id)
        if group is not None:
            return group(group['data'])
        return None
    
    def print_groups(self):
        groups = self.get_groups()
        if groups is not None:
            for group in groups:
                self.logger.info('Group Id: ' + group.id + ' Name: ' + group.name + ' Created At: ' + group.created_at)

    def get_hybrid_deployment_agents(self):
        resp = self.call_api('GET', 'hybrid-deployment-agents', {'limit': 100})
        if resp is not None:
            hybrid_deployment_agents = []
            for item in resp['data']['items']:
                hybrid_deployment_agents.append(hybrid_deployment_agent(item))
            return hybrid_deployment_agents
        return None
    
    def print_hybrid_deployment_agents(self, hybrid_deployment_agents=None):
        if hybrid_deployment_agents is None:
            hybrid_deployment_agents = self.get_hybrid_deployment_agents()
        for hybrid_deployment_agent in hybrid_deployment_agents:
            self.logger.info('Hybrid Deployment Agent Id: ' + hybrid_deployment_agent.id + ' Display Name: ' + hybrid_deployment_agent.display_name + ' Group Id: ' + hybrid_deployment_agent.group_id + ' Registered At: ' + hybrid_deployment_agent.registered_at + ' Usage: ' + hybrid_deployment_agent.usage)

    def get_private_link_detail(self, private_link_id):
        prvtlnk = self.call_api('GET', 'private-links/' + private_link_id)
        if prvtlnk is not None:
            return privatelink(prvtlnk['data'])
        return None

    def get_private_links(self):
        prvtlnks = self.call_api('GET', 'private-links', {'limit': 100})
        if prvtlnks is not None:
            privatelinks = []
            for _privatelink in prvtlnks['data']['items']:
                privatelinks.append(privatelink(_privatelink))
            return privatelinks
        return None
    
    def print_private_links(self):
        privatelinks = self.get_private_links()
        if privatelinks is not None:
            for privatelink in privatelinks:
                self.logger.info('Private Link Id: ' + privatelink.id + ' Name: ' + privatelink.name + ' Region: ' + privatelink.region + ' Service: ' + privatelink.service + ' State: ' + privatelink.state + ' Account Id: ' + privatelink.account_id + ' Created At: ' + privatelink.created_at + ' Created By: ' + privatelink.created_by + ' Cloud Provider: ' + privatelink.cloud_provider + ' State Summary: ' + privatelink.state_summary + ' Config: ' + privatelink.config)

    def create_private_link(self, payload):
        prvtlnk = self.call_api('POST', 'private-links', payload)
        if prvtlnk is not None:
            return privatelink(prvtlnk['data'])
        return None
    
    def delete_private_link(self, private_link_id):
        prvtlnk = self.call_api('DELETE', 'private-links/' + private_link_id)
        if prvtlnk is not None:
            return privatelink(prvtlnk['data'])
        return None
    
    def update_private_link(self, private_link_id, payload):
        prvtlnk = self.call_api('PATCH', 'private-links/' + private_link_id, payload)
        if prvtlnk is not None:
            return privatelink(prvtlnk['data'])
        return None

    def get_user_detail(self, user_id):
        usr = self.call_api('GET', 'users/' + user_id)
        if usr is not None:
            return user(usr['data'])
        return None

    def get_users(self):
        usrs = self.call_api('GET', 'users')
        if usrs is not None:
            users = []
            for _user in usrs['data']['items']:
                users.append(user(_user))
            return users
        return None
    
    def print_users(self):
        users = self.get_users()
        if users is not None:
            for user in users:
                self.logger.info('User Id: ' + user.id + ' Email: ' + user.email + ' Verified: ' + user.verified + ' Invited: ' + user.invited + ' Picture: ' + user.picture + ' Phone: ' + user.phone + ' Role: ' + user.role + ' Active: ' + user.active + ' Given Name: ' + user.given_name + ' Created At: ' + user.created_at + ' Family Name: ' + user.family_name + ' Logged In At: ' + user.logged_in_at)
    
