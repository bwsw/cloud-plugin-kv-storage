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

    ConfigKey<String> KvStorageElasticSearchList = new ConfigKey<>("Advanced", String.class, "storage.kv.elasticsearch.list", null,
            "Comma separated list of ElasticSearch HTTP hosts; e.g. http://localhost,http://localhost:9201", false);

    ConfigKey<Integer> KvStorageMaxNameLength = new ConfigKey<>("Advanced", Integer.class, "storage.kv.name.length.max", "256", "Max name length for account storages", true);

    ConfigKey<Integer> KvStorageMaxDescriptionLength = new ConfigKey<>("Advanced", Integer.class, "storage.kv.description.length.max", "1024",
            "Max description length for account storages", true);

    ConfigKey<Integer> KvStorageMaxTtl = new ConfigKey<>("Advanced", Integer.class, "storage.kv.ttl.max", "3600000", "Max ttl in ms for temporal storages", true);

    KvStorage createAccountStorage(Long accountId, String name, String description);

    ListResponse<KvStorageResponse> listStorages(Long accountId, Long startIndex, Long pageSize);

    String createTempStorage(Integer ttl);

    String createVmStorage(Long vmId);
}
