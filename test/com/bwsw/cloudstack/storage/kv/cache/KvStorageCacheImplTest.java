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
import com.cloud.exception.PermissionDeniedException;
import com.cloud.user.Account;
import com.cloud.user.AccountManager;
import com.cloud.user.AccountVO;
import com.cloud.user.User;
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import com.google.common.cache.LoadingCache;
import org.apache.cloudstack.acl.SecurityChecker;
import org.apache.cloudstack.context.CallContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
    private static final AccountVO ACCOUNT_VO = new AccountVO();
    private static final KvStorage ACCOUNT_STORAGE = new KvStorage(ID, UUID, "test", null, false);
    private static final KvStorage VM_STORAGE = new KvStorage(ID, false);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private LoadingCache<String, Optional<KvStorage>> _cache;

    @Mock
    private AccountManager _accountManager;

    @Mock
    private AccountDao _accountDao;

    @Mock
    private VMInstanceDao _vmInstanceDao;

    @Mock
    private User _callerUser;

    @Mock
    private Account _callerAccount;

    @Mock
    private VMInstanceVO _vmInstanceVO;

    @InjectMocks
    private KvStorageCacheImpl _kvStorageCache;

    @BeforeClass
    public static void beforeClass() {
        CallContext.unregisterAll();
    }

    @AfterClass
    public static void afterClass() {
        CallContext.unregisterAll();
    }

    @Test
    public void testGetNonexistentStorage() throws ExecutionException {
        when(_cache.get(ID)).thenReturn(Optional.empty());

        Optional<KvStorage> result = testGet();
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetCacheException() throws ExecutionException {
        expectException(ExecutionException.class, InvalidEntityException.class.getName());
        when(_cache.get(ID)).thenThrow(new ExecutionException(new InvalidEntityException()));

        testGet();
    }

    @Test
    public void testGetTempStorage() throws ExecutionException {
        Optional<KvStorage> cachedStorage = Optional.of(new KvStorage(ID, 60000, System.currentTimeMillis()));
        when(_cache.get(ID)).thenReturn(cachedStorage);

        Optional<KvStorage> result = testGet();
        assertEquals(cachedStorage, result);
    }

    @Test
    public void testGetVmStorage() throws ExecutionException {
        Optional<KvStorage> cachedStorage = Optional.of(VM_STORAGE);
        when(_cache.get(ID)).thenReturn(cachedStorage);
        when(_vmInstanceDao.findByUuid(VM_STORAGE.getId())).thenReturn(_vmInstanceVO);
        doNothing().when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, _vmInstanceVO);

        Optional<KvStorage> result = testGet();
        assertEquals(cachedStorage, result);
    }

    @Test
    public void testGetVmStorageDeletedVm() throws ExecutionException {
        when(_cache.get(ID)).thenReturn(Optional.of(VM_STORAGE));
        when(_vmInstanceDao.findByUuid(VM_STORAGE.getId())).thenReturn(null);

        Optional<KvStorage> result = testGet();
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetVmStoragePermissionDenied() throws ExecutionException {
        PermissionDeniedException exception = new PermissionDeniedException("VM");
        expectException(exception.getClass(), exception.getMessage());
        when(_cache.get(ID)).thenReturn(Optional.of(VM_STORAGE));
        when(_vmInstanceDao.findByUuid(VM_STORAGE.getId())).thenReturn(_vmInstanceVO);
        doThrow(exception).when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, _vmInstanceVO);

        testGet();
    }

    @Test
    public void testGetAccountStorage() throws ExecutionException {
        Optional<KvStorage> cachedStorage = Optional.of(ACCOUNT_STORAGE);
        when(_cache.get(ID)).thenReturn(cachedStorage);
        when(_accountDao.findByUuid(ACCOUNT_STORAGE.getAccount())).thenReturn(ACCOUNT_VO);
        doNothing().when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, ACCOUNT_VO);

        Optional<KvStorage> result = testGet();
        assertEquals(cachedStorage, result);
    }

    @Test
    public void testGetAccountStorageDeletedAccount() throws ExecutionException {
        when(_cache.get(ID)).thenReturn(Optional.of(ACCOUNT_STORAGE));
        when(_accountDao.findByUuid(ACCOUNT_STORAGE.getAccount())).thenReturn(null);

        Optional<KvStorage> result = testGet();
        assertEquals(Optional.empty(), result);
    }

    @Test
    public void testGetAccountStoragePermissionDenied() throws ExecutionException {
        PermissionDeniedException exception = new PermissionDeniedException("account");
        expectException(exception.getClass(), exception.getMessage());
        when(_cache.get(ID)).thenReturn(Optional.of(ACCOUNT_STORAGE));
        when(_accountDao.findByUuid(ACCOUNT_STORAGE.getAccount())).thenReturn(ACCOUNT_VO);
        doThrow(exception).when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, ACCOUNT_VO);

        testGet();
    }

    private Optional<KvStorage> testGet() throws ExecutionException {
        CallContext.register(_callerUser, _callerAccount);
        return _kvStorageCache.get(ID);
    }

    private void expectException(Class<? extends Throwable> exception, String message) {
        expectedException.expect(exception);
        expectedException.expectMessage(message);
    }
}
