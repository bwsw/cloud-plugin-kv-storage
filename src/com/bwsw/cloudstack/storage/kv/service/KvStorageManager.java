// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.bwsw.cloudstack.storage.kv.service;

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.response.KvStorageResponse;
import com.cloud.utils.component.PluggableService;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.cloudstack.framework.config.ConfigKey;

public interface KvStorageManager extends PluggableService {

    ConfigKey<String> KvStorageElasticsearchList = new ConfigKey<>("Advanced", String.class, "storage.kv.elasticsearch.list", null,
            "Comma separated list of ElasticSearch HTTP hosts; e.g. http://localhost,http://localhost:9201", false);

    ConfigKey<String> KvStorageElasticsearchUsername = new ConfigKey<>("Advanced", String.class, "storage.kv.elasticsearch.username", null,
            "Elasticsearch username for authentication", false);

    ConfigKey<String> KvStorageElasticsearchPassword = new ConfigKey<>("Advanced", String.class, "storage.kv.elasticsearch.password", null,
            "Elasticsearch password for authentication", false);

    ConfigKey<Integer> KvStorageMaxNameLength = new ConfigKey<>("Advanced", Integer.class, "storage.kv.name.length.max", "256", "Max name length for account storages", true);

    ConfigKey<Integer> KvStorageMaxDescriptionLength = new ConfigKey<>("Advanced", Integer.class, "storage.kv.description.length.max", "1024",
            "Max description length for account storages", true);

    ConfigKey<Boolean> KvStorageVmHistoryEnabled = new ConfigKey<>("Advanced", Boolean.class, "storage.kv.vm.history.enabled", "false",
            "if VM storages should keep an operation history, false otherwise", true);

    // account storages
    KvStorage createAccountStorage(Long accountId, String name, String description, Boolean historyEnabled);

    ListResponse<KvStorageResponse> listAccountStorages(Long accountId, Long startIndex, Long pageSize);

    boolean deleteAccountStorage(Long accountId, String storageId);

    // temp storages
    KvStorage createTempStorage(Integer ttl);

    KvStorage updateTempStorage(String storageId, Integer ttl);

    boolean deleteTempStorage(String storageId);

    void expireTempStorages();

    // vm storages
    KvStorage createVmStorage(String vmId);

    boolean deleteVmStorage(String vmId);

    void deleteExpungedVmStorages();

    // utilities

    void cleanupStorages();
}
