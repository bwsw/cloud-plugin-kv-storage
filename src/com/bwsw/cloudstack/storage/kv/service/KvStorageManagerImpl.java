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

import com.bwsw.cloudstack.storage.kv.api.CreateAccountKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.CreateTempKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.DeleteAccountKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.DeleteTempKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.ListAccountKvStoragesCmd;
import com.bwsw.cloudstack.storage.kv.api.UpdateTempKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.entity.CreateStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.entity.ScrollableListResponse;
import com.bwsw.cloudstack.storage.kv.job.KvStorageJobManager;
import com.bwsw.cloudstack.storage.kv.response.KvStorageResponse;
import com.bwsw.cloudstack.storage.kv.util.HttpUtils;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.utils.component.ComponentLifecycleBase;
import com.cloud.utils.db.SearchBuilder;
import com.cloud.utils.db.SearchCriteria;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.apache.cloudstack.framework.config.Configurable;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KvStorageManagerImpl extends ComponentLifecycleBase implements KvStorageManager, Configurable {

    private static final Logger s_logger = Logger.getLogger(KvStorageManagerImpl.class);

    private static final int DELETE_BATCH_SIZE = 100;
    private static final int DELETE_BATCH_TIMEOUT = 300000; // 5 minutes
    private static final String UUID_IN_CONDITION = "uuid_in";

    @Inject
    private AccountDao _accountDao;

    @Inject
    private VMInstanceDao _vmInstanceDao;

    @Inject
    private KvRequestBuilder _kvRequestBuilder;

    @Inject
    private KvExecutor _kvExecutor;

    @Inject
    private KvStorageJobManager _kvStorageJobManager;

    private RestHighLevelClient _restHighLevelClient;

    private SearchBuilder<VMInstanceVO> _vmInstanceVOSearchBuilder;

    @Override
    public KvStorage createAccountStorage(Long accountId, String name, String description, Boolean historyEnabled) {
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
        if (historyEnabled == null) {
            historyEnabled = false;
        }
        KvStorage storage = new KvStorage(UUID.randomUUID().toString(), accountVO.getUuid(), name, description, historyEnabled);
        return createStorage(storage);
    }

    @Override
    public ListResponse<KvStorageResponse> listAccountStorages(Long accountId, Long startIndex, Long pageSize) {
        if (pageSize == null || pageSize < 1) {
            throw new InvalidParameterValueException("Invalid page size");
        }
        if (startIndex == null) {
            startIndex = 0L;
        } else if (startIndex < 0) {
            throw new InvalidParameterValueException("Invalid start index");
        }
        AccountVO accountVO = _accountDao.findById(accountId);
        if (accountVO == null) {
            throw new InvalidParameterValueException("Unable to find an account with the specified id");
        }
        SearchRequest searchRequest = _kvRequestBuilder.getSearchRequest(accountVO.getUuid(), startIndex.intValue(), pageSize.intValue());
        try {
            return _kvExecutor.search(_restHighLevelClient, searchRequest, KvStorageResponse.class);
        } catch (IOException e) {
            s_logger.error("Unable to retrieve storage", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to retrieve storages", e);
        }
    }

    @Override
    public boolean deleteAccountStorage(Long accountId, String storageId) {
        AccountVO accountVO = _accountDao.findById(accountId);
        if (accountVO == null) {
            throw new InvalidParameterValueException("Unable to find an account with the specified id");
        }
        return deleteStorage(storageId, storage -> {
            if (!KvStorage.KvStorageType.ACCOUNT.equals(storage.getType())) {
                throw new InvalidParameterValueException("The storage type is not account");
            }
            if (storage.getAccount() == null || !storage.getAccount().equals(accountVO.getUuid())) {
                throw new InvalidParameterValueException("The storage does not belong to the specified account");
            }
        });
    }

    @Override
    public KvStorage createTempStorage(Integer ttl) {
        checkTtl(ttl);
        KvStorage storage = new KvStorage(UUID.randomUUID().toString(), ttl, KvStorage.getCurrentTimestamp() + ttl);
        return createStorage(storage);
    }

    @Override
    public KvStorage updateTempStorage(String storageId, Integer ttl) {
        if (storageId == null || storageId.isEmpty()) {
            throw new InvalidParameterValueException("Invalid storage id");
        }
        checkTtl(ttl);
        GetRequest getRequest = _kvRequestBuilder.getGetRequest(storageId);
        try {
            KvStorage storage = _kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class);
            if (storage == null) {
                throw new InvalidParameterValueException("The storage does not exist");
            }
            if (!KvStorage.KvStorageType.TEMP.equals(storage.getType())) {
                throw new InvalidParameterValueException("The storage type is not temp");
            }
            storage.setExpirationTimestamp(storage.getExpirationTimestamp() - storage.getTtl() + ttl);
            storage.setTtl(ttl);
            _kvExecutor.update(_restHighLevelClient, _kvRequestBuilder.getUpdateTTLRequest(storage));
            return storage;
        } catch (IOException e) {
            s_logger.error("Unable to update a storage", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to update the storages", e);
        }
    }

    @Override
    public boolean deleteTempStorage(String storageId) {
        return deleteStorage(storageId, storage -> {
            if (!KvStorage.KvStorageType.TEMP.equals(storage.getType())) {
                throw new InvalidParameterValueException("The storage type is not temp");
            }
        });
    }

    @Override
    public void expireTempStorages() {
        try {
            Request request = _kvRequestBuilder.getExpireTempStorageRequest(KvStorage.getCurrentTimestamp());
            Response response = _restHighLevelClient.getLowLevelClient().performRequest(request.getMethod(), request.getEndpoint(), request.getParameters(), request.getEntity());
            if (response.getStatusLine().getStatusCode() == RestStatus.OK.getStatus()) {
                s_logger.info("Temp storages have been expired");
            } else {
                s_logger.error("Unexpected status while expiring temp storages " + response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            s_logger.error("Unable to expire temp storages", e);
        }
    }

    @Override
    public KvStorage createVmStorage(String vmId) {
        VMInstanceVO vmInstanceVO = _vmInstanceDao.findByUuid(vmId);
        if (vmInstanceVO == null) {
            throw new InvalidParameterValueException("Unable to find a VM with the specified id");
        }
        Boolean historyEnabled = KvStorageVmHistoryEnabled.value();
        if (historyEnabled == null) {
            historyEnabled = false;
        }
        KvStorage storage = new KvStorage(vmInstanceVO.getUuid(), historyEnabled);
        return createStorage(storage);
    }

    @Override
    public boolean deleteVmStorage(String vmId) {
        VMInstanceVO vmInstanceVO = _vmInstanceDao.findByUuidIncludingRemoved(vmId);
        if (vmInstanceVO == null) {
            throw new InvalidParameterValueException("Unable to find a VM with the specified id");
        }
        return deleteStorage(vmId, storage -> {
            if (!KvStorage.KvStorageType.VM.equals(storage.getType())) {
                throw new InvalidParameterValueException("The storage type is not VM");
            }
        });
    }

    @Override
    public void deleteExpungedVmStorages() {
        SearchRequest searchRequest = _kvRequestBuilder.getVmStoragesRequest(DELETE_BATCH_SIZE, DELETE_BATCH_TIMEOUT);
        try {
            ScrollableListResponse<KvStorage> response = _kvExecutor.scroll(_restHighLevelClient, searchRequest, KvStorage.class);
            while (response != null && response.getResults() != null && !response.getResults().isEmpty()) {

                SearchCriteria<VMInstanceVO> searchCriteria = _vmInstanceVOSearchBuilder.create();
                searchCriteria.setParameters(UUID_IN_CONDITION, response.getResults().stream().map(KvStorage::getId).toArray());
                List<VMInstanceVO> vmInstanceVOList = _vmInstanceDao.searchIncludingRemoved(searchCriteria, null, null, false);
                Map<String, VMInstanceVO> vmByUuid;
                if (vmInstanceVOList != null) {
                    vmByUuid = vmInstanceVOList.stream().collect(Collectors.toMap(VMInstanceVO::getUuid, Function.identity()));
                } else {
                    vmByUuid = new HashMap<>();
                }

                for (KvStorage storage : response.getResults()) {
                    VMInstanceVO vmInstanceVO = vmByUuid.get(storage.getId());
                    if (vmInstanceVO == null || vmInstanceVO.isRemoved()) {
                        s_logger.info("Delete the storage for the expunged VM " + storage.getId());
                        storage.setDeleted(true);
                        _kvExecutor.update(_restHighLevelClient, _kvRequestBuilder.getMarkDeletedRequest(storage));
                    }
                }
                response = _kvExecutor.scroll(_restHighLevelClient, _kvRequestBuilder.getScrollRequest(response.getScrollId(), DELETE_BATCH_TIMEOUT), KvStorage.class);
            }
        } catch (Exception e) {
            s_logger.error("Unable to delete storages for expunged VMs", e);
        }
    }

    @Override
    public void cleanupStorages() {
        SearchRequest searchRequest = _kvRequestBuilder.getDeletedStoragesRequest(DELETE_BATCH_SIZE, DELETE_BATCH_TIMEOUT);
        try {
            ScrollableListResponse<KvStorage> response = _kvExecutor.scroll(_restHighLevelClient, searchRequest, KvStorage.class);
            while (response != null && response.getResults() != null && !response.getResults().isEmpty()) {
                for (KvStorage storage : response.getResults()) {
                    s_logger.info("Clean up the storage " + storage.getId());
                    _kvExecutor.delete(_restHighLevelClient, _kvRequestBuilder.getDeleteRequest(storage));
                }
                response = _kvExecutor.scroll(_restHighLevelClient, _kvRequestBuilder.getScrollRequest(response.getScrollId(), DELETE_BATCH_TIMEOUT), KvStorage.class);
            }
        } catch (Exception e) {
            s_logger.error("Unable to cleanup storages", e);
        }

    }

    @Override
    public List<Class<?>> getCommands() {
        List<Class<?>> commands = new ArrayList<>();
        commands.add(ListAccountKvStoragesCmd.class);
        commands.add(CreateAccountKvStorageCmd.class);
        commands.add(DeleteAccountKvStorageCmd.class);
        commands.add(CreateTempKvStorageCmd.class);
        commands.add(UpdateTempKvStorageCmd.class);
        commands.add(DeleteTempKvStorageCmd.class);
        return commands;
    }

    @Override
    public String getConfigComponentName() {
        return KvStorageManager.class.getSimpleName();
    }

    @Override
    public ConfigKey<?>[] getConfigKeys() {
        return new ConfigKey[] {KvStorageElasticsearchList, KvStorageElasticsearchUsername, KvStorageElasticsearchPassword, KvStorageMaxNameLength, KvStorageMaxDescriptionLength};
    }

    @Override
    public boolean configure(String name, Map<String, Object> params) {
        try {
            RestClientBuilder restClientBuilder = RestClient.builder(HttpUtils.getHttpHosts(KvStorageElasticsearchList.value()).toArray(new HttpHost[] {}));
            String username = KvStorageElasticsearchUsername.value();
            if (!Strings.isNullOrEmpty(username)) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, KvStorageElasticsearchPassword.value()));
                restClientBuilder = restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            _restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        } catch (IllegalArgumentException e) {
            s_logger.error("Failed to create ElasticSearch client", e);
            return false;
        }
        _kvStorageJobManager.init(this, _restHighLevelClient);

        _vmInstanceVOSearchBuilder = _vmInstanceDao.createSearchBuilder();
        _vmInstanceVOSearchBuilder.and(UUID_IN_CONDITION, _vmInstanceVOSearchBuilder.entity().getUuid(), SearchCriteria.Op.IN);
        return true;
    }

    private KvStorage createStorage(KvStorage storage) {
        try {
            CreateStorageRequest request = _kvRequestBuilder.getCreateRequest(storage);
            _kvExecutor.create(_restHighLevelClient, request);
        } catch (IOException e) {
            s_logger.error("Unable to create a storage", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to create a storage", e);
        }
        return storage;
    }

    private void checkTtl(Integer ttl) {
        if (ttl == null) {
            throw new InvalidParameterValueException("Unspecified TTL");
        }
        if (ttl <= 0) {
            throw new InvalidParameterValueException("Invalid TTL");
        }
    }

    private boolean deleteStorage(String storageId, Consumer<KvStorage> validator) {
        if (storageId == null || storageId.isEmpty()) {
            throw new InvalidParameterValueException("Invalid storage id");
        }
        GetRequest getRequest = _kvRequestBuilder.getGetRequest(storageId);
        try {
            KvStorage storage = _kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class);
            if (storage == null) {
                throw new InvalidParameterValueException("The storage does not exist");
            }
            validator.accept(storage);
            storage.setDeleted(true);
            return _kvExecutor.delete(_restHighLevelClient, _kvRequestBuilder.getDeleteRequest(storage));
        } catch (IOException e) {
            s_logger.error("Unable to delete the KV storage", e);
            return false;
        }
    }
}
