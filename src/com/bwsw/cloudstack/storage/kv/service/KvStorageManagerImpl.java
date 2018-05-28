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
import com.bwsw.cloudstack.storage.kv.util.HttpUtils;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.utils.component.ComponentLifecycleBase;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.apache.cloudstack.framework.config.Configurable;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;

import javax.inject.Inject;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class KvStorageManagerImpl extends ComponentLifecycleBase implements KvStorageManager, Configurable {

    private static final Logger s_logger = Logger.getLogger(KvStorageManagerImpl.class);

    @Inject
    private AccountDao _accountDao;

    @Inject
    private VMInstanceDao _vmInstanceDao;

    @Inject
    private KvRequestBuilder _kvRequestBuilder;

    @Inject
    private KvExecutor _kvExecutor;

    private RestHighLevelClient _restHighLevelClient;

    @Override
    public String createAccountStorage(Long accountId, String name, String description) {
        AccountVO accountVO = _accountDao.findById(accountId);
        if (accountVO == null) {
            throw new InvalidParameterValueException("Unable to find an account with the specified id");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new InvalidParameterValueException("Unspecified name");
        }
        Integer maxNameLength = KvStorageMaxNameLength.value();
        if (maxNameLength != null && name.length() > maxNameLength) {
            throw new InvalidParameterValueException("Invalid name, max length is " + maxNameLength);
        }
        Integer maxDescriptionLength = KvStorageMaxDescriptionLength.value();
        if (description != null && maxDescriptionLength != null && description.length() > maxDescriptionLength) {
            throw new InvalidParameterValueException("Invalid description, max length is " + maxDescriptionLength);
        }
        KvStorage storage = new KvStorage(UUID.randomUUID().toString(), accountVO.getUuid(), name, description);
        return createStorage(storage);
    }

    @Override
    public String createTempStorage(Integer ttl) {
        if (ttl == null) {
            throw new InvalidParameterValueException("Unspecified TTL");
        }
        Integer maxTtl = KvStorageMaxTtl.value();
        if (ttl <= 0 || maxTtl != null && ttl > maxTtl) {
            throw new InvalidParameterValueException("Invalid TTL");
        }
        KvStorage storage = new KvStorage(UUID.randomUUID().toString(), ttl, Instant.now().toEpochMilli() + ttl);
        return createStorage(storage);
    }

    @Override
    public String createVmStorage(Long vmId) {
        VMInstanceVO vmInstanceVO = _vmInstanceDao.findById(vmId);
        if (vmInstanceVO == null) {
            throw new InvalidParameterValueException("Unable to find a virtual machine with specified id");
        }
        KvStorage storage = new KvStorage(vmInstanceVO.getUuid());
        return createStorage(storage);
    }

    @Override
    public List<Class<?>> getCommands() {
        List<Class<?>> commands = new ArrayList<>();
        return commands;
    }

    @Override
    public String getConfigComponentName() {
        return KvStorageManager.class.getSimpleName();
    }

    @Override
    public ConfigKey<?>[] getConfigKeys() {
        return new ConfigKey[] {KvStorageElasticSearchList, KvStorageMaxNameLength, KvStorageMaxDescriptionLength, KvStorageMaxTtl};
    }

    @Override
    public boolean configure(String name, Map<String, Object> params) {
        try {
            _restHighLevelClient = new RestHighLevelClient(RestClient.builder(HttpUtils.getHttpHosts(KvStorageElasticSearchList.value()).toArray(new HttpHost[] {})));
        } catch (IllegalArgumentException e) {
            s_logger.error("Failed to create ElasticSearch client", e);
            return false;
        }
        return true;
    }

    private String createStorage(KvStorage storage) {
        try {
            IndexRequest request = _kvRequestBuilder.getCreateRequest(storage);
            _kvExecutor.index(_restHighLevelClient, request);
        } catch (IOException e) {
            s_logger.error("Unable to create a storage", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to create a storage", e);
        }
        return storage.getId();
    }
}
