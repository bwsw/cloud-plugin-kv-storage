Apache CloudStack Plugin for key/value storage
==============

This project provides API plugin for Apache CloudStack to manage [key/value storages](https://git.bw-sw.com/cloudstack-ecosystem/cs-kv-storage).
The version of the plugin matches Apache CloudStack version that it is build for.

The plugin is developed and tested only with Apache CloudStack 4.11.2.

* [Installing into CloudStack](#installing-into-cloudstack)
* [Plugin settings](#plugin-settings)
* [Deployment](#deployment)
* [API](#api)

# Installing into CloudStack

Download the plugin jar with dependencies file from OSS Nexus (https://oss.sonatype.org/content/groups/public/com/bwsw/cloud-plugin-kv-storage/) which corresponds to your ACS 
version (e.g. 4.11.2.0), put it to lib directory and restart Management server. In Ubuntu installation which is based on deb package:

```
cd /usr/share/cloudstack-management/lib/
wget --trust-server-names "https://oss.sonatype.org/service/local/artifact/maven/redirect?r=snapshots&g=com.bwsw&a=cloud-plugin-kv-storage&c=jar-with-dependencies&v=4.11.2.0-SNAPSHOT"
service cloudstack-management stop
service cloudstack-management start
```

Event bus should be configured in CloudStack. The documentation can be found at http://docs.cloudstack.apache.org/projects/cloudstack-administration/en/4.11/events.html
Recommended event buses are InMemoryEventBus (built-in) and [HybridEventBus](https://github.com/bwsw/cloud-plugin-event-bus-hybrid).
The plugin does not work with built-in KafkaEventBus and RabbitMQEventBus.  

# Plugin settings

| Name | Description | Default value |
| -------------- | ----------- | -------- |
| storage.kv.elasticsearch.list | comma separated list of Elasticsearch HTTP hosts; e.g. http://localhost,http://localhost:9201 | |
| storage.kv.elasticsearch.username | Elasticsearch username for authentication; should be empty if authentication is disabled | |
| storage.kv.elasticsearch.password | Elasticsearch password for authentication; should be empty if authentication is disabled | |
| storage.kv.vm.history.enabled | true if VM storages should keep an operation history, false otherwise | false |
| storage.kv.cache.size.max | maximum size of storage cache | 10000 |
| storage.kv.url | KV storage URL | |
| storage.kv.url.public | public KV storage URL | |

*default.page.size* is used as a default value for pagesize parameter in [listAccountKvStorages](#listaccountkvstorages) command. Its value should be less or equal to Elasticsearch 
*index.max_result_window* otherwise listAccountKvStorages requests without pagesize parameter will fail.

# Deployment

Following components should be deployed:

* cs-kv-storage

The documentation can be found at https://git.bw-sw.com/cloudstack-ecosystem/cs-kv-storage

# API

* [Storage management](#storage-management)
* [Stotage operations](#storage-operations)
* [Storage history](#storage-history)

## Storage management

The plugin provides following API commands to manage key/value storages:

* [createAccountKvStorage (A)](#createaccountkvstorage)
* [deleteAccountKvStorage (A)](#deleteaccountkvstorage)
* [listAccountKvStorages](#listaccountkvstorages)
* [createTempKvStorage (A)](#createtempkvstorage)
* [updateTempKvStorage](#updatetempkvstorage)
* [deleteTempKvStorage (A)](#deletetempkvstorage)
* [getKvStorage](#getkvstorage)
* [regenerateKvStorageSecretKey](#regeneratekvstoragesecretkey)

(A) implies that the command is asynchronous.

### createAccountKvStorage

Creates an account KV storage.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| accountid | the ID of the account to create a storage for | true |
| name | the storage name | true |
| description | the storage description | false |
| historyenabled | true if the storage should keep an operation history, false otherwise. Default value - false. | false |

**Response tags**

See [storage response tages](#storage-response-tags).

### deleteAccountKvStorage

Deletes an account KV storage.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| accountid | the ID of the account associated with the storage | true |
| storageid | the ID of the storage to delete | true |

**Response tags**

See [async command response tags](#async-command-response-tags).

### listAccountKvStorages

Lists storages associated with the account.

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| accountid | the ID of the account associated with the storage | true |
| page | the page number of results | false |
| pagesize | the number of results returned in the page | false |

**Response tags**

See [storage response tages](#storage-response-tags).

### createTempKvStorage

Creates a temporal KV storage.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| ttl | TTL in milliseconds | true |

**Response tags**

See [storage response tages](#storage-response-tags).

### updateTempKvStorage

Updates a temporal KV storage.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage to update | true |
| ttl | TTL in milliseconds | true |

**Response tags**

See [storage response tages](#storage-response-tags).

### deleteTempKvStorage

Deletes a temporal KV storage.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage to delete | true |

**Response tags**

See [async command response tags](#async-command-response-tags).

### getKvStorage

Retrieves a KV storage by id.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |

**Response tags**

See [storage response tages](#storage-response-tags).

### regenerateKvStorageSecretKey

Regenerates a secret key for a KV storage. It takes some time to accept new secret key while executing storage operations.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |

**Response tags**

See [storage response tages](#storage-response-tags).

## Storage operations

* [getKvStorageValue](#getKvStorageValue)
* [getKvStorageValues](#getKvStorageValues)
* [setKvStorageValue](#setkvstoragevalue)
* [setKvStorageValues](#setkvstoragevalues)
* [deleteKvStorageKey](#deletekvstoragekey)
* [deleteKvStorageKeys](#deletekvstoragekeys)
* [listKvStorageKeys](#listkvstoragekeys)
* [clearKvStorage](#clearkvstorage)

In all storage operation commands if the request is invalid or an error occurs while processing it the standard error response is returned.

### getKvStorageValue

Gets the value by the key.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| key | the key to retrieve value for | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;value | the value associated with the key |
| kverror | failure response (see [KV error response tags](#kv-error-response-tags)). If the key does not exist 404 code is returned. |

### getKvStorageValues

Get values by keys.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| keys | keys to retrieve value for | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;items | key/value pairs as the map |

### setKvStorageValue

Sets the value for the key.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| key | the key | true |
| value | the value | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;key | the key |
| &nbsp;value | the value |

### setKvStorageValues

Sets values for the keys.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| items | key/value pairs; should be specified in the request in the way items[0].key1=value1&items[0].key2=value2 | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;items | keys associated with boolean values (as a result of set operation) as map  |

### deleteKvStorageKey

Removes the key.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| key | the key to be deleted | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;key | the key  |

### deleteKvStorageKeys

Removes keys.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| keys | keys to be deleted | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;items | keys associated with boolean values (as a result of set operation) as map |

### listKvStorageKeys

Lists keys.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;items | keys in the storage as a collection |

### clearKvStorage

Clears the storage.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |

**Response tags**

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;success | always true if kvresult is present |
| kverror | failure response (see [KV error response tags](#kv-error-response-tags)). |

## Storage history

* [getKvStorageHistory](#getkvstoragehistory)
* [scrollKvStorageHistory](#scrollkvstoragehistory)

### getKvStorageHistory

Searches and lists history records.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| storageid | the ID of the storage | true |
| keys | comma separated list of keys | no |
| operations | comma separated list of operations. Possible values are set, delete or clear. | no |
| start | the start date/time as Unix timestamp in ms to retrieve history records with dates >= start | no |
| end | the end date/time as Unix timestamp in ms to retrieve history records with dates <= end | no |
| sort | comma separated list of response fields optionally prefixed with - (minus) for descending order. | no |
| page | a page number of results (1 by default) | no |
| size | a number of results returned in the page/batch (default value the same as in cs-kv-storage) | no |
| scroll | a timeout in ms for subsequent [scroll requests](#scrollkvstoragehistory) | no |

\* `start` and `end` parameters can be used separately. If both `start` and `end` parameters are specified history
records with dates that are greater/equal to `start` and less/equal to `end` are returned.

\** If both `page` and `scroll` parameters are specified `scroll` is used.

**Response tags**

See [KV history response tags](#kv-history-response-tags).

### scrollKvStorageHistory

Retrieves next batch of history records.

**Request parameters**

| Parameter Name | Description | Required |
| -------------- | ----------- | -------- |
| scrollid | scroll id to retrieve next batch of history records | yes |
| timeout | timeout in ms for subsequent scroll requests | yes |

**Response tags**

See [KV history response tags](#kv-history-response-tags).

## Response tags

### Storage response tags

| Response Name | Description |
| -------------- | ---------- |
| id | the ID of the storage |
| type | the type of the storage. Possible values are ACCOUNT, TEMP, VM. |
| deleted | true if the storage is deleted, false otherwise |
| account | the account associated with the storage |
| name | the name of the storage |
| description | the description of the storage |
| historyenabled | true if the storage should keep an operation history, false otherwise |
| secretkey | the secret key to be used for storage operations |
| url | public KV storage URL |
| lastupdated | timestamp of last storage update by storage management operations |

### Async command response tags

| Response Name | Description |
| -------------- | ---------- |
| displaytext | any text associated with success or failure |
| success | true if operation is executed successfully |

### KV error response tags

| Response Name | Description |
| -------------- | ---------- |
| code | error code |

### KV history response tags

| Response Name | Description |
| -------------- | ---------- |
| kvresult | success response |
| &nbsp;&nbsp;&nbsp;&nbsp;total | the total number of history records |
| &nbsp;&nbsp;&nbsp;&nbsp;page | page number for requests without scrolling |
| &nbsp;&nbsp;&nbsp;&nbsp;size | the number of history records (items tag) |
| &nbsp;&nbsp;&nbsp;&nbsp;scrollid | scroll id or subsequent [scroll requests](#scrollkvstoragehistory) for requests with scrolling |
| &nbsp;&nbsp;&nbsp;&nbsp;items(*) | history records |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;key | the key |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;value | the value |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;operation | the operation executed. Possible values are set, delete or clear. |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;timestamp | date/time as Unix timestamp in ms when the operation was executed |
