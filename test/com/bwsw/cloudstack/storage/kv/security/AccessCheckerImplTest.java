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

package com.bwsw.cloudstack.storage.kv.security;

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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AccessCheckerImplTest {

    private static final KvStorage ACCOUNT_STORAGE = new KvStorage("35e7200d-0fda-4ca9-ad3e-3b3b37a77e32", "account secret", "38a8e712-d77d-4b6f-ab17-b330c7bd959b", "test", null,
            false);
    private static final KvStorage VM_STORAGE = new KvStorage("07dee574-e12c-420b-b0f1-3943267d8664", "vm secret", false);
    private static final KvStorage TEMP_STORAGE = new KvStorage("d0e3fa08-446b-4c70-9ddb-1738e7da45ac", "vm secret", 60000, System.currentTimeMillis());
    private static final AccountVO ACCOUNT_VO = new AccountVO();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
    private AccessCheckerImpl _accessChecker;

    @BeforeClass
    public static void beforeClass() {
        CallContext.unregisterAll();
    }

    @AfterClass
    public static void afterClass() {
        CallContext.unregisterAll();
    }

    @Test
    public void testCheckTempStorage() {
        testCheck(TEMP_STORAGE);

        verifyZeroInteractions(_accountManager);
    }

    @Test
    public void testCheckVmStorage() {
        when(_vmInstanceDao.findByUuid(VM_STORAGE.getId())).thenReturn(_vmInstanceVO);
        doNothing().when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, _vmInstanceVO);

        testCheck(VM_STORAGE);
    }

    @Test
    public void testCheckVmStorageNonexistentVm() {
        expectedException.expect(InvalidEntityException.class);
        when(_vmInstanceDao.findByUuid(VM_STORAGE.getId())).thenReturn(null);

        testCheck(VM_STORAGE);
    }

    @Test
    public void testCheckVmStoragePermissionDenied() {
        PermissionDeniedException exception = new PermissionDeniedException("VM");
        expectException(exception);
        when(_vmInstanceDao.findByUuid(VM_STORAGE.getId())).thenReturn(_vmInstanceVO);
        doThrow(exception).when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, _vmInstanceVO);

        testCheck(VM_STORAGE);
    }

    @Test
    public void testCheckAccountStorage() {
        when(_accountDao.findByUuid(ACCOUNT_STORAGE.getAccount())).thenReturn(ACCOUNT_VO);
        doNothing().when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, ACCOUNT_VO);

        testCheck(ACCOUNT_STORAGE);
    }

    @Test
    public void testCheckAccountStorageNonexistentAccount() {
        expectedException.expect(InvalidEntityException.class);
        when(_accountDao.findByUuid(ACCOUNT_STORAGE.getAccount())).thenReturn(null);

        testCheck(ACCOUNT_STORAGE);
    }

    @Test
    public void testCheckAccountStoragePermissionDenied() {
        PermissionDeniedException exception = new PermissionDeniedException("account");
        expectException(exception);
        when(_accountDao.findByUuid(ACCOUNT_STORAGE.getAccount())).thenReturn(ACCOUNT_VO);
        doThrow(exception).when(_accountManager).checkAccess(_callerAccount, SecurityChecker.AccessType.OperateEntry, false, ACCOUNT_VO);

        testCheck(ACCOUNT_STORAGE);
    }

    private void testCheck(KvStorage storage) {
        CallContext.register(_callerUser, _callerAccount);
        _accessChecker.check(storage);
    }

    private void expectException(Throwable throwable) {
        expectedException.expect(throwable.getClass());
        expectedException.expectMessage(throwable.getMessage());
    }
}
