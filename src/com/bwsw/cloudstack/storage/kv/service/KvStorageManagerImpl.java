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

import com.bwsw.cloudstack.storage.kv.api.ClearKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.CreateAccountKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.CreateTempKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.DeleteAccountKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.DeleteKvStorageKeyCmd;
import com.bwsw.cloudstack.storage.kv.api.DeleteKvStorageKeysCmd;
import com.bwsw.cloudstack.storage.kv.api.DeleteTempKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.GetKvStorageValueCmd;
import com.bwsw.cloudstack.storage.kv.api.GetKvStorageValuesCmd;
import com.bwsw.cloudstack.storage.kv.api.GetVmKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.api.ListAccountKvStoragesCmd;
import com.bwsw.cloudstack.storage.kv.api.ListKvStorageKeysCmd;
import com.bwsw.cloudstack.storage.kv.api.RegenerateKvStorageSecretKeyCmd;
import com.bwsw.cloudstack.storage.kv.api.SetKvStorageValueCmd;
import com.bwsw.cloudstack.storage.kv.api.SetKvStorageValuesCmd;
import com.bwsw.cloudstack.storage.kv.api.UpdateTempKvStorageCmd;
import com.bwsw.cloudstack.storage.kv.cache.KvStorageCache;
import com.bwsw.cloudstack.storage.kv.cache.KvStorageCacheFactory;
import com.bwsw.cloudstack.storage.kv.entity.CreateStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.entity.ScrollableListResponse;
import com.bwsw.cloudstack.storage.kv.exception.InvalidEntityException;
import com.bwsw.cloudstack.storage.kv.job.KvStorageJobManager;
import com.bwsw.cloudstack.storage.kv.response.KvKey;
import com.bwsw.cloudstack.storage.kv.response.KvKeys;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import com.bwsw.cloudstack.storage.kv.response.KvPair;
import com.bwsw.cloudstack.storage.kv.response.KvResult;
import com.bwsw.cloudstack.storage.kv.response.KvStorageResponse;
import com.bwsw.cloudstack.storage.kv.security.AccessChecker;
import com.bwsw.cloudstack.storage.kv.security.KeyGenerator;
import com.bwsw.cloudstack.storage.kv.util.HttpUtils;
import com.cloud.exception.InvalidParameterValueException;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.utils.component.ComponentLifecycleBase;
import com.cloud.utils.db.GenericDao;
import com.cloud.utils.db.SearchBuilder;
import com.cloud.utils.db.SearchCriteria;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.Identity;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.cloudstack.framework.config.ConfigKey;
import org.apache.cloudstack.framework.config.Configurable;
import org.apache.commons.lang.time.DateUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class KvStorageManagerImpl extends ComponentLifecycleBase implements KvStorageManager, Configurable {

    private static final Logger s_logger = Logger.getLogger(KvStorageManagerImpl.class);

    private static final int DELETE_BATCH_SIZE = 100;
    private static final int DELETE_BATCH_TIMEOUT = 300000; // 5 minutes
    private static final String UUID_IN_CONDITION = "uuid_in";
    private static final String REMOVED_GTE_CONDITION = "removed_gte";

    @FunctionalInterface
    private interface ExceptionalSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface RequestBuilder<T> {
        Request get(List<T> uuids) throws Exception;
    }

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

    @Inject
    private KvStorageCacheFactory _kvStorageCacheFactory;

    @Inject
    private KeyGenerator _keyGenerator;

    @Inject
    private AccessChecker _accessChecker;

    private KvOperationManager _kvOperationManager;

    private RestHighLevelClient _restHighLevelClient;

    private SearchBuilder<VMInstanceVO> _vmInstanceVOByUuidSearchBuilder;

    private SearchBuilder<VMInstanceVO> _vmInstanceVOByRemovedSearchBuilder;

    private SearchBuilder<AccountVO> _accountVOByUuidSearchBuilder;

    private KvStorageCache _kvStorageCache;

    @Override
    public KvStorage createAccountStorage(Long accountId, String name, String description, Boolean historyEnabled) {
        AccountVO accountVO = _accountDao.findById(accountId);
        if (accountVO == null) {
            throw new InvalidParameterValueException("Unable to find an account with the specified id");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new InvalidParameterValueException("Unspecified name");
        }
        if (historyEnabled == null) {
            historyEnabled = false;
        }
        KvStorage storage = new KvStorage(UUID.randomUUID().toString(), _keyGenerator.generate(), accountVO.getUuid(), name, description, historyEnabled);
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
    public void deleteAccountStorages(String accountUuid) {
        AccountVO accountVO = _accountDao.findByUuidIncludingRemoved(accountUuid);
        if (accountVO == null) {
            throw new InvalidParameterValueException("Unable to find an account with the specified id");
        }
        SearchRequest searchRequest = _kvRequestBuilder.getAccountStoragesRequest(accountVO.getUuid(), DELETE_BATCH_SIZE, DELETE_BATCH_TIMEOUT);
        try {
            ScrollableListResponse<KvStorage> response = _kvExecutor.scroll(_restHighLevelClient, searchRequest, KvStorage.class);
            while (response != null && response.getResults() != null && !response.getResults().isEmpty()) {
                for (KvStorage storage : response.getResults()) {
                    storage.setDeleted(true);
                    s_logger.info("Deleting the KV storage " + storage.getId() + " for the account " + storage.getAccount());
                    if (!_kvExecutor.delete(_restHighLevelClient, _kvRequestBuilder.getDeleteRequest(storage))) {
                        s_logger.error("Unable to delete account KV storage " + storage.getId());
                    } else {
                        s_logger.info("The KV storage " + storage.getId() + " for the account " + storage.getAccount() + " has been deleted");
                    }
                }
                response = _kvExecutor.scroll(_restHighLevelClient, _kvRequestBuilder.getScrollRequest(response.getScrollId(), DELETE_BATCH_TIMEOUT), KvStorage.class);
            }
        } catch (Exception e) {
            s_logger.error("Failed to delete storages for an account " + accountVO.getUuid(), e);
        }
    }

    @Override
    public void deleteAccountStoragesForDeletedAccounts() {
        markDeleteEntityRelatedStorages(() -> _kvRequestBuilder.getAccountStoragesRequest(DELETE_BATCH_SIZE, DELETE_BATCH_TIMEOUT), _accountVOByUuidSearchBuilder, _accountDao,
                KvStorage::getAccount, account -> account.getRemoved() != null);
    }

    @Override
    public void deleteAccountStoragesForRecentlyDeletedAccount(int interval) {
        markDeletedStorageForDeletedEntities(() -> _accountDao.findRecentlyDeletedAccounts(null, DateUtils.addMilliseconds(new Date(), -interval), null),
                uuids -> _kvRequestBuilder.getMarkDeletedAccountStorageRequest(uuids), AccountVO.class);
    }

    @Override
    public KvStorage createTempStorage(Integer ttl) {
        checkTtl(ttl);
        KvStorage storage = new KvStorage(UUID.randomUUID().toString(), _keyGenerator.generate(), ttl, KvStorage.getCurrentTimestamp() + ttl);
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
            if (storage == null || storage.getDeleted() != null && storage.getDeleted()) {
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
        KvStorage storage = new KvStorage(vmInstanceVO.getUuid(), _keyGenerator.generate(), historyEnabled);
        return createStorage(storage);
    }

    @Override
    public KvStorage getOrCreateVmStorage(String vmId) {
        GetRequest getRequest = _kvRequestBuilder.getGetRequest(vmId);
        try {
            KvStorage storage = _kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class);
            if (storage == null) {
                storage = createVmStorage(vmId);
            }
            return storage;
        } catch (IOException e) {
            s_logger.error("Unable to get/create a storage for VM " + vmId, e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to get/create a storage for VM " + vmId, e);
        }
    }

    @Override
    public KvStorage getVmStorage(Long vmId) {
        VMInstanceVO vmInstanceVO = _vmInstanceDao.findById(vmId);
        if (vmInstanceVO == null) {
            throw new InvalidParameterValueException("Unable to find a VM with the specified id");
        }
        try {
            return getStorage(vmInstanceVO.getUuid());
        } catch (IOException e) {
            s_logger.error("Unable to retrieve a storage", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to retrieve VM storage", e);
        }
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
        markDeleteEntityRelatedStorages(() -> _kvRequestBuilder.getVmStoragesRequest(DELETE_BATCH_SIZE, DELETE_BATCH_TIMEOUT), _vmInstanceVOByUuidSearchBuilder, _vmInstanceDao,
                KvStorage::getId, VMInstanceVO::isRemoved);
    }

    @Override
    public void deleteVmStoragesForRecentlyDeletedVms(int interval) {
        markDeletedStorageForDeletedEntities(() -> {
            SearchCriteria<VMInstanceVO> searchCriteria = _vmInstanceVOByRemovedSearchBuilder.create();
            searchCriteria.setParameters(REMOVED_GTE_CONDITION, DateUtils.addMilliseconds(new Date(), -interval));
            return _vmInstanceDao.searchIncludingRemoved(searchCriteria, null, null, false);
        }, uuids -> _kvRequestBuilder.getMarkDeletedVmStorageRequest(uuids), VMInstanceVO.class);
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
    public KvStorage regenerateSecretKey(String storageId) {
        try {
            KvStorage storage = getStorage(storageId);
            _accessChecker.check(storage);
            storage.setSecretKey(_keyGenerator.generate());
            UpdateRequest request = _kvRequestBuilder.getUpdateSecretKey(storage);
            _kvExecutor.update(_restHighLevelClient, request);
            return storage;
        }catch (InvalidEntityException e) {
            throw new InvalidParameterValueException("The storage does not exist");
        } catch (IOException e) {
            s_logger.error("Unable to regenerate a secret key for KV storage " + storageId, e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to regenerate a secret key for KV storage " + storageId, e);
        }
    }

    @Override
    public KvOperationResponse getValue(String storageId, String key) {
        return execute(storageId, storage -> _kvOperationManager.get(storage, key));
    }

    @Override
    public KvOperationResponse getValues(String storageId, Collection<String> keys) {
        return execute(storageId, storage -> _kvOperationManager.get(storage, keys));
    }

    @Override
    public KvPair setValue(String storageId, String key, String value) {
        return execute(storageId, storage -> _kvOperationManager.set(storage, key, value));
    }

    @Override
    public KvResult setValues(String storageId, Map<String, String> data) {
        return execute(storageId, storage -> _kvOperationManager.set(storage, data));
    }

    @Override
    public KvKey deleteKey(String storageId, String key) {
        return execute(storageId, storage -> _kvOperationManager.delete(storage, key));
    }

    @Override
    public KvResult deleteKeys(String storageId, Collection<String> keys) {
        return execute(storageId, storage -> _kvOperationManager.delete(storage, keys));
    }

    @Override
    public KvKeys listKeys(String storageId) {
        return execute(storageId, storage -> _kvOperationManager.list(storage));
    }

    @Override
    public KvOperationResponse clear(String storageId) {
        return execute(storageId, storage -> _kvOperationManager.clear(storage));
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
        commands.add(GetKvStorageValueCmd.class);
        commands.add(GetKvStorageValuesCmd.class);
        commands.add(SetKvStorageValueCmd.class);
        commands.add(SetKvStorageValuesCmd.class);
        commands.add(DeleteKvStorageKeyCmd.class);
        commands.add(DeleteKvStorageKeysCmd.class);
        commands.add(ListKvStorageKeysCmd.class);
        commands.add(ClearKvStorageCmd.class);
        commands.add(GetVmKvStorageCmd.class);
        commands.add(RegenerateKvStorageSecretKeyCmd.class);
        return commands;
    }

    @Override
    public String getConfigComponentName() {
        return KvStorageManager.class.getSimpleName();
    }

    @Override
    public ConfigKey<?>[] getConfigKeys() {
        return new ConfigKey[] {KvStorageElasticsearchList, KvStorageElasticsearchUsername, KvStorageElasticsearchPassword, KvStorageVmHistoryEnabled, KvStorageCacheMaxSize,
                KvStorageUrl};
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

        _vmInstanceVOByUuidSearchBuilder = _vmInstanceDao.createSearchBuilder();
        _vmInstanceVOByUuidSearchBuilder.and(UUID_IN_CONDITION, _vmInstanceVOByUuidSearchBuilder.entity().getUuid(), SearchCriteria.Op.IN);

        _accountVOByUuidSearchBuilder = _accountDao.createSearchBuilder();
        _accountVOByUuidSearchBuilder.and(UUID_IN_CONDITION, _accountVOByUuidSearchBuilder.entity().getUuid(), SearchCriteria.Op.IN);

        _vmInstanceVOByRemovedSearchBuilder = _vmInstanceDao.createSearchBuilder();
        _vmInstanceVOByRemovedSearchBuilder.and(_vmInstanceVOByRemovedSearchBuilder.entity().getRemoved(), SearchCriteria.Op.NNULL);
        _vmInstanceVOByRemovedSearchBuilder.and(REMOVED_GTE_CONDITION, _vmInstanceVOByRemovedSearchBuilder.entity().getRemoved(), SearchCriteria.Op.GTEQ);

        _kvStorageCache = _kvStorageCacheFactory.getCache(KvStorageCacheMaxSize.value(), _restHighLevelClient);
        _kvOperationManager = new KvOperationManagerImpl(KvStorageUrl.value());

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
            s_logger.error("Unable to delete the KV storage " + storageId, e);
            return false;
        }
    }

    private <T extends Identity> void markDeleteEntityRelatedStorages(Supplier<SearchRequest> requestBuilder, SearchBuilder<T> searchBuilder, GenericDao<T, Long> dao,
            Function<KvStorage, String> entityUuidRetriever, Predicate<T> removedChecker) {
        SearchRequest searchRequest = requestBuilder.get();
        try {
            ScrollableListResponse<KvStorage> response = _kvExecutor.scroll(_restHighLevelClient, searchRequest, KvStorage.class);
            while (response != null && response.getResults() != null && !response.getResults().isEmpty()) {
                SearchCriteria<T> searchCriteria = searchBuilder.create();
                searchCriteria.setParameters(UUID_IN_CONDITION, response.getResults().stream().map(entityUuidRetriever).toArray());
                List<T> entityList = dao.searchIncludingRemoved(searchCriteria, null, null, false);
                Map<String, T> entityByUuid;
                if (entityList != null) {
                    entityByUuid = entityList.stream().collect(Collectors.toMap(T::getUuid, Function.identity()));
                } else {
                    entityByUuid = new HashMap<>();
                }
                for (KvStorage storage : response.getResults()) {
                    T entity = entityByUuid.get(entityUuidRetriever.apply(storage));
                    if (entity == null || removedChecker.test(entity)) {
                        s_logger.info("Deleting " + storage.getType().name() + " storage " + storage.getId() + " for the removed entity " + entityUuidRetriever.apply(storage));
                        storage.setDeleted(true);
                        _kvExecutor.update(_restHighLevelClient, _kvRequestBuilder.getMarkDeletedRequest(storage));
                        s_logger.info("Deleted " + storage.getType().name() + " storage " + storage.getId() + " for the removed entity " + entityUuidRetriever.apply(storage));
                    }
                }
                response = _kvExecutor.scroll(_restHighLevelClient, _kvRequestBuilder.getScrollRequest(response.getScrollId(), DELETE_BATCH_TIMEOUT), KvStorage.class);
            }
        } catch (Exception e) {
            s_logger.error("Error while deleting storages for removed entities", e);
        }
    }

    private <T extends Identity> void markDeletedStorageForDeletedEntities(ExceptionalSupplier<List<T>> entitySupplier, RequestBuilder<String> requestBuilder,
            Class<T> entityClass) {
        try {
            List<T> entities = entitySupplier.get();
            if (entities != null && !entities.isEmpty()) {
                Request request = requestBuilder.get(entities.stream().map(T::getUuid).collect(Collectors.toList()));
                Response response = _restHighLevelClient.getLowLevelClient()
                        .performRequest(request.getMethod(), request.getEndpoint(), request.getParameters(), request.getEntity());
                if (response.getStatusLine().getStatusCode() == RestStatus.OK.getStatus()) {
                    s_logger.info("KV storages for recently removed " + entityClass.getSimpleName() + " have been cleaned");
                } else {
                    s_logger.error(
                            "Unexpected status while cleaning KV storages for recently removed " + entityClass.getSimpleName() + " " + response.getStatusLine().getStatusCode());
                }
            }
        } catch (Exception e) {
            s_logger.error("Unable to cleanup KV storages for recently removed " + entityClass.getSimpleName(), e);
        }
    }

    private <T extends KvOperationResponse> T execute(String storageId, Function<KvStorage, T> retriever) {
        Optional<KvStorage> storage;
        try {
            storage = _kvStorageCache.get(storageId);
            if (!storage.isPresent()) {
                throw new InvalidParameterValueException("KV storage does not exist");
            }
        } catch (ExecutionException | UncheckedExecutionException e) {
            s_logger.error("Unable to execute storage operation", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to execute KV storage operation");
        }
        return retriever.apply(storage.get());
    }

    private KvStorage getStorage(String storageId) throws IOException {
        GetRequest getRequest = _kvRequestBuilder.getGetRequest(storageId);
        KvStorage storage = _kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class);
        if (storage == null || storage.getDeleted() != null && storage.getDeleted()) {
            throw new InvalidParameterValueException("The storage does not exist");
        }
        return storage;
    }
}
