Apache CloudStack Plugin for key/value storage
==============

This project provides API plugin for Apache CloudStack to manage [key/value storages](https://git.bw-sw.com/cloudstack-ecosystem/cs-kv-storage).
The version of the plugin matches Apache CloudStack version that it is build for.

* [API](#api)
* [Plugin settings](#plugin-settings)
* [Deployment](#deployment)

# API

The plugin provides following API commands to manage key/value storages:

* [createAccountKvStorage (A)](#createaccountkvstorage)
* [deleteAccountKvStorage (A)](#deleteaccountkvstorage)
* [listAccountKvStorages](#listaccountkvstorages)
* [createTempKvStorage (A)](#createtempkvstorage)
* [updateTempKvStorage](#updatetempkvstorage)
* [deleteTempKvStorage (A)](#deletetempkvstorage)

(A) implies that the command is asynchronous.

## Commands

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

### Async command response tags

| Response Name | Description |
| -------------- | ---------- |
| displaytext | any text associated with success or failure |
| success | true if operation is executed successfully |

# Plugin settings

| Name | Description | Default value |
| -------------- | ----------- | -------- |
| storage.kv.elasticsearch.list | comma separated list of Elasticsearch HTTP hosts; e.g. http://localhost,http://localhost:9201 | |
| storage.kv.elasticsearch.username | Elasticsearch username for authentication; should be empty if authentication is disabled | |
| storage.kv.elasticsearch.password | Elasticsearch password for authentication; should be empty if authentication is disabled | |
| storage.kv.name.length.max | max name length for account storages | 256 |
| storage.kv.description.length.max | max description length for account storages | 1024 |
| storage.kv.vm.history.enabled | true if VM storages should keep an operation history, false otherwise | false |

# Deployment

Following components should be deployed:

* cs-kv-storage

The documentation can be found at https://git.bw-sw.com/cloudstack-ecosystem/cs-kv-storage
