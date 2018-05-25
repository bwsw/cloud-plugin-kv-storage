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
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.apache.cloudstack.framework.config.Configurable;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class KvStorageManagerImpl implements KvStorageManager, Configurable {

    private static final Logger s_logger = Logger.getLogger(KvStorageManagerImpl.class);

    @Inject
    private AccountDao _accountDao;

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
        try {
            IndexRequest request = _kvRequestBuilder.getCreateRequest(storage);
            _kvExecutor.index(_restHighLevelClient, request);
        } catch (IOException e) {
            s_logger.error("Unable to create an account storage", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to create a storage", e);
        }
        return storage.getId();
    }

    @Override
    public String createTempStorage(Integer ttl) {
        return null;
    }

    @Override
    public String createVmStorage(Long vmId) {
        return null;
    }

    @Override
    public List<Class<?>> getCommands() {
        return null;
    }

    @Override
    public String getConfigComponentName() {
        return KvStorageManager.class.getSimpleName();
    }

    @Override
    public ConfigKey<?>[] getConfigKeys() {
        return new ConfigKey[] {KvStorageElasticSearchList, KvStorageMaxNameLength, KvStorageMaxDescriptionLength};
    }
}
