# Fivetran API Python wrapper

Extendable Python wrapper for [Fivetran](https://www.fivetran.com/)'s [REST API](https://fivetran.com/docs/rest-api/api-reference/) - reusing snippets of code from Fivetran's [api_framework examples](https://github.com/fivetran/api_framework/blob/main/examples/). 

Current wrapper implements the functionality of retrieving account info, connections, destinations, groups, hybrid deployment agents, privatelinks, and users. It also implements create/delete connector.  

## Usage

```python
import fivetranapi

ft = fivetranapi.connect('api_key', 'api_secret')
ft.print_account_info()
```
The `connect` class can be instantiated with a different api-endpoint for testing purposes. 
### account info

```python
account_info = ft.get_account_info()
print('Account Id: ' + account_info.account_id)
print('Account Name: ' + account_info.account_name)
print('System Key Id: ' + account_info.system_key_id)
print('User Id: ' + account_info.user_id)
```
Calls the [account information endpoint](https://fivetran.com/docs/rest-api/api-reference/account/get-account-info) and returns an an object of the account class with ways to retrieve the configuration.

```python
ft.print_account_info()
```
Logs the information using the logging object. 

### connection

```python
connections = ft.get_connections()
for connection in connections:
    # ...
ft.print_connections(connections)
```
Provides a way to access all the connections from the [API](https://fivetran.com/docs/rest-api/api-reference/connections/list-connections). The `print_connections` method takes an optional array of connection objects to avoid re-querying the API unless necessary.

### connectors

```python
resp = ft.get_connectors()
# print out the valid connectors?
```

Uses the [metadata-connectors](https://fivetran.com/docs/rest-api/api-reference/connector-metadata/metadata-connectors) endpoint to retrieve all the valid connectors.

### connector payload

```python
resp = ft.get_connector_schema('azure_sql_db')
pretty_json = json.dumps(resp, indent=4)
print(pretty_json)
```

Uses the [metadata-connector-config](https://fivetran.com/docs/rest-api/api-reference/connector-metadata/metadata-connector-config) endpoint to retrieve the configuration parameters for a connector type which will help craft the payload for creating a connector. 

### create connector

```python
payload = {...}
resp = ft.create_connector(payload)
# .. handle errors
```
To create a connector you have to provide an appropriate [payload](https://fivetran.com/docs/rest-api/api-reference/connections/create-connection?service=azure_sql_db) based on the Fivetran documentation for each connector type. 

### delete connector

```python
resp = ft.delete_connector('connector_id')
# .. handle errors
```

The `delete_connector` takes a connector_id as the only input. 

### destinations

```python
destinations = ft.get_destinations()
for destination in destinations:
    #...
# retrieve specific destination
destination = ft.get_destination_detail('destination_id')
ft.print_destinations()
```
Uses the [list-destinations](https://fivetran.com/docs/rest-api/api-reference/destinations/list-destinations) and [destination-details](https://fivetran.com/docs/rest-api/api-reference/destinations/destination-details) endpoinsts to retrieve either all destinations or a specific destination. 

### groups

```python
groups = ft.get_groups()
for group in groups:
    #... 
ft.print_groups()
```

Uses the [list-all-groups](https://fivetran.com/docs/rest-api/api-reference/groups/list-all-groups) endpoint to retrieve and log all groups. 

### hybrid deployment agents

```python
hdas = ft.get_hybrid_deployment_agents()
for hda in hdas:
    #...
ft.print_hybrid_deployment_agents()
```

Uses the [get-local-processing-agent-list](https://fivetran.com/docs/rest-api/api-reference/hybrid-deployment-agent-management/get-local-processing-agent-list) endpoint to retrieve and log all hybrid deployment agents.

### private links

```python
privatelinks = ft.get_private_links()
for privatelink in privatelinks:
    #...
privatelink = ft.get_private_link_detail('private_link_id')
ft.print_private_links()
```

Uses the [get-private-links](https://fivetran.com/docs/rest-api/api-reference/private-links/get-private-links) and [get-private-link-details](https://fivetran.com/docs/rest-api/api-reference/private-links/get-private-link-details) endpoints to retrieve and log all private links.

### users

```python
users = ft.get_users()
for user in users:
    #...
user = ft.get_user_detail('user_id')
ft.print_users()
```

Uses the [list-all-users](https://fivetran.com/docs/rest-api/api-reference/users/list-all-users) and [user-details](https://fivetran.com/docs/rest-api/api-reference/users/user-details) to retrieve and log all users.

## Dependencies

Dependencies can be installed from the included [requirements.txt](requirements.txt)

Currently consists of logging and requests. 

## Todo 

- [ ] Handle cursors in responses - but I have no endpoint which paginates at the moment. 

## Author

[Thomas Eibner](https://github.com/thomaseibner/) [LinkedIn](https://www.linkedin.com/in/thomaseibner/)

## Credits 

Mohammed Sabhi @ Ecolab
Elijah Davis @ Fivetran [fivetran-elijahdavis](https://github.com/fivetran-elijahdavis/)

## License

Licensed under the MIT License. 