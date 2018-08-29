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
import com.bwsw.cloudstack.storage.kv.security.AccessChecker;
import com.cloud.exception.PermissionDeniedException;
import com.google.common.cache.LoadingCache;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KvStorageCacheImplTest {

    private static final String ID = "e0123777-921b-4e62-a7cc-8135015ca571";
    private static final String UUID = "35e7200d-0fda-4ca9-ad3e-3b3b37a77e32";
    private static final String SECRET_KEY = "secret";
    private static final KvStorage ACCOUNT_STORAGE = new KvStorage(ID, SECRET_KEY, UUID, "test", null, false);
    private static final KvStorage VM_STORAGE = new KvStorage(ID, SECRET_KEY, false);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private LoadingCache<String, Optional<KvStorage>> _cache;

    @Mock
    private AccessChecker _accessChecker;

    @InjectMocks
    private KvStorageCacheImpl _kvStorageCache;

    @Test
    public void testGetNonexistentStorage() throws ExecutionException {
        when(_cache.get(ID)).thenReturn(Optional.empty());

        Optional<KvStorage> result = _kvStorageCache.get(ID);
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetCacheException() throws ExecutionException {
        expectException(ExecutionException.class, InvalidEntityException.class.getName());
        when(_cache.get(ID)).thenThrow(new ExecutionException(new InvalidEntityException()));

        _kvStorageCache.get(ID);
    }

    @Test
    public void testGetTempStorage() throws ExecutionException {
        testGet(new KvStorage(ID, SECRET_KEY, 60000, System.currentTimeMillis()));
    }

    @Test
    public void testGetVmStorage() throws ExecutionException {
        testGet(VM_STORAGE);
    }

    @Test
    public void testGetVmStorageDeletedVm() throws ExecutionException {
        testGetInvalidEntity(VM_STORAGE);
    }

    @Test
    public void testGetVmStoragePermissionDenied() throws ExecutionException {
        testGetPermissionDenied(VM_STORAGE);
    }

    @Test
    public void testGetAccountStorage() throws ExecutionException {
        testGet(ACCOUNT_STORAGE);
    }

    @Test
    public void testGetAccountStorageDeletedAccount() throws ExecutionException {
        testGetInvalidEntity(ACCOUNT_STORAGE);
    }

    @Test
    public void testGetAccountStoragePermissionDenied() throws ExecutionException {
        testGetPermissionDenied(ACCOUNT_STORAGE);
    }

    private void testGet(KvStorage storage) throws ExecutionException {
        Optional<KvStorage> cachedStorage = Optional.of(storage);
        when(_cache.get(ID)).thenReturn(cachedStorage);
        doNothing().when(_accessChecker).check(storage);

        Optional<KvStorage> result = _kvStorageCache.get(ID);
        assertEquals(cachedStorage, result);
    }

    private void testGetInvalidEntity(KvStorage storage) throws ExecutionException {
        when(_cache.get(ID)).thenReturn(Optional.of(storage));
        doThrow(new InvalidEntityException()).when(_accessChecker).check(storage);

        Optional<KvStorage> result = _kvStorageCache.get(ID);
        assertEquals(Optional.empty(), result);
    }

    private void testGetPermissionDenied(KvStorage storage) throws ExecutionException {
        PermissionDeniedException exception = new PermissionDeniedException(storage.getType().name());
        expectException(exception.getClass(), exception.getMessage());
        when(_cache.get(ID)).thenReturn(Optional.of(storage));
        doThrow(exception).when(_accessChecker).check(storage);

        _kvStorageCache.get(ID);
    }

    private void expectException(Class<? extends Throwable> exception, String message) {
        expectedException.expect(exception);
        expectedException.expectMessage(message);
    }
}
