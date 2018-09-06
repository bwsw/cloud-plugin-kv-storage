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
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.dao.VMInstanceDao;
import org.apache.cloudstack.acl.ControlledEntity;
import org.apache.cloudstack.acl.SecurityChecker;
import org.apache.cloudstack.context.CallContext;

import javax.inject.Inject;

public class AccessCheckerImpl implements AccessChecker {

    @Inject
    private AccountDao _accountDao;

    @Inject
    private VMInstanceDao _vmInstanceDao;

    @Inject
    private AccountManager _accountManager;

    @Override
    public void check(KvStorage storage) throws PermissionDeniedException, InvalidEntityException {
        switch (storage.getType()) {
        case VM:
            check(_vmInstanceDao.findByUuid(storage.getId()));
            break;
        case ACCOUNT:
            check(_accountDao.findByUuid(storage.getAccount()));
            break;
        case TEMP:
            break;
        }
    }

    private void check(ControlledEntity entity) {
        if (entity == null) {
            throw new InvalidEntityException();
        }
        Account caller = CallContext.current().getCallingAccount();
        _accountManager.checkAccess(caller, SecurityChecker.AccessType.OperateEntry, false, entity);
    }
}
