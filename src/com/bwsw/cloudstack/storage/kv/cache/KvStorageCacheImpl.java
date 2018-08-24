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
import com.cloud.user.Account;
import com.cloud.user.AccountManager;
import com.cloud.user.AccountVO;
import com.cloud.user.dao.AccountDao;
import com.cloud.vm.VMInstanceVO;
import com.cloud.vm.dao.VMInstanceDao;
import com.google.common.cache.LoadingCache;
import org.apache.cloudstack.acl.SecurityChecker;
import org.apache.cloudstack.context.CallContext;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class KvStorageCacheImpl implements KvStorageCache {

    private final LoadingCache<String, Optional<KvStorage>> _cache;
    private final AccountManager _accountManager;
    private final AccountDao _accountDao;
    private final VMInstanceDao _vmInstanceDao;

    KvStorageCacheImpl(LoadingCache<String, Optional<KvStorage>> cache, AccountManager accountManager, AccountDao accountDao, VMInstanceDao vmInstanceDao) {
        this._cache = cache;
        this._accountManager = accountManager;
        this._accountDao = accountDao;
        this._vmInstanceDao = vmInstanceDao;
    }

    public Optional<KvStorage> get(String id) throws ExecutionException {
        Account caller = CallContext.current().getCallingAccount();
        Optional<KvStorage> cachedStorage = _cache.get(id);
        if (cachedStorage.isPresent()) {
            KvStorage storage = cachedStorage.get();
            switch (storage.getType()) {
            case VM:
                VMInstanceVO vmInstanceVO = _vmInstanceDao.findByUuid(storage.getId());
                if (vmInstanceVO == null) {
                    return Optional.empty();
                }
                _accountManager.checkAccess(caller, SecurityChecker.AccessType.OperateEntry, false, vmInstanceVO);
                break;
            case ACCOUNT:
                AccountVO accountVO = _accountDao.findByUuid(storage.getAccount());
                if (accountVO == null) {
                    return Optional.empty();
                }
                _accountManager.checkAccess(caller, SecurityChecker.AccessType.OperateEntry, false, accountVO);
                break;
            case TEMP:
                break;
            }
        }
        return cachedStorage;
    }

}
