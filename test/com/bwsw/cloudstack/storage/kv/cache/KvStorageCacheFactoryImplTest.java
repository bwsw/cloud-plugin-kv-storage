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

package com.bwsw.cloudstack.storage.kv.cache;

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.exception.InvalidEntityException;
import com.bwsw.cloudstack.storage.kv.service.KvExecutor;
import com.bwsw.cloudstack.storage.kv.service.KvRequestBuilder;
import com.cloud.user.AccountManager;
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.dao.VMInstanceDao;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KvStorageCacheFactoryImplTest {

    private static final String ID = "e0123777-921b-4e62-a7cc-8135015ca571";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AccountManager _accountManager;

    @Mock
    private AccountDao _accountDao;

    @Mock
    private VMInstanceDao _vmInstanceDao;

    @Mock
    private KvRequestBuilder _kvRequestBuilder;

    @Mock
    private KvExecutor _kvExecutor;

    @Mock
    private RestHighLevelClient _restHighLevelClient;

    @Mock
    private GetRequest getRequest;

    @InjectMocks
    private KvStorageCacheFactoryImpl _kvStorageCacheFactory;

    @SuppressWarnings("unchecked")
    @Test
    public void testGetCache() {
        KvStorageCache cache = _kvStorageCacheFactory.getCache(100, _restHighLevelClient);

        assertNotNull(cache);
        Object innerCacheObject = ReflectionTestUtils.getField(cache, "_cache");
        assertTrue(innerCacheObject instanceof LoadingCache);
        assertSame(_accountManager, ReflectionTestUtils.getField(cache, "_accountManager"));
        assertSame(_accountDao, ReflectionTestUtils.getField(cache, "_accountDao"));
        assertSame(_vmInstanceDao, ReflectionTestUtils.getField(cache, "_vmInstanceDao"));
    }

    @Test
    public void testGetCacheLoadValue() throws IOException, ExecutionException {
        LoadingCache<String, Optional<KvStorage>> innerCache = getInnerCache();

        KvStorage storage = getStorage(KvStorage.KvStorageType.VM, false);
        when(_kvRequestBuilder.getGetRequest(storage.getId())).thenReturn(getRequest);
        when(_kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class)).thenReturn(storage);

        Optional<KvStorage> result = innerCache.get(storage.getId());
        assertTrue(result.isPresent());
        assertSame(storage, result.get());
    }

    @Test
    public void testGetCacheLoadValueNonexistentStorage() throws IOException, ExecutionException {
        LoadingCache<String, Optional<KvStorage>> innerCache = getInnerCache();

        when(_kvRequestBuilder.getGetRequest(ID)).thenReturn(getRequest);
        when(_kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class)).thenReturn(null);

        Optional<KvStorage> result = innerCache.get(ID);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetCacheLoadValueDeletedStorage() throws IOException, ExecutionException {
        LoadingCache<String, Optional<KvStorage>> innerCache = getInnerCache();

        KvStorage storage = getStorage(KvStorage.KvStorageType.VM, true);
        when(_kvRequestBuilder.getGetRequest(storage.getId())).thenReturn(getRequest);
        when(_kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class)).thenReturn(storage);

        Optional<KvStorage> result = innerCache.get(storage.getId());
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetCacheLoadValueNullStorageType() throws IOException, ExecutionException {
        testGetCacheLoadInvalidResult(getStorage(null, false));
    }

    @Test
    public void testGetCacheLoadValueNullDeleted() throws IOException, ExecutionException {
        testGetCacheLoadInvalidResult(getStorage(KvStorage.KvStorageType.VM, null));
    }

    @Test
    public void testGetCacheLoadValueExecutorException() throws IOException, ExecutionException {
        expectedException.expect(ExecutionException.class);
        expectedException.expectMessage(IOException.class.getName());

        LoadingCache<String, Optional<KvStorage>> innerCache = getInnerCache();
        when(_kvRequestBuilder.getGetRequest(ID)).thenReturn(getRequest);
        when(_kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class)).thenThrow(new IOException());

        innerCache.get(ID);
    }

    private void testGetCacheLoadInvalidResult(KvStorage storage) throws IOException, ExecutionException {
        expectedException.expect(UncheckedExecutionException.class);
        expectedException.expectMessage(InvalidEntityException.class.getName());

        LoadingCache<String, Optional<KvStorage>> innerCache = getInnerCache();

        when(_kvRequestBuilder.getGetRequest(storage.getId())).thenReturn(getRequest);
        when(_kvExecutor.get(_restHighLevelClient, getRequest, KvStorage.class)).thenReturn(storage);

        innerCache.get(storage.getId());
    }

    @SuppressWarnings("unchecked")
    private LoadingCache<String, Optional<KvStorage>> getInnerCache() {
        KvStorageCache cache = _kvStorageCacheFactory.getCache(100, _restHighLevelClient);

        assertNotNull(cache);
        Object innerCacheObject = ReflectionTestUtils.getField(cache, "_cache");
        assertTrue(innerCacheObject instanceof LoadingCache);
        return (LoadingCache<String, Optional<KvStorage>>)innerCacheObject;
    }

    private KvStorage getStorage(KvStorage.KvStorageType type, Boolean deleted) {
        KvStorage storage = new KvStorage();
        storage.setId(ID);
        storage.setType(type);
        storage.setDeleted(deleted);
        return storage;
    }
}
