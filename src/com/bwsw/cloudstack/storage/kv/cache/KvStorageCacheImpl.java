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
import com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class KvStorageCacheImpl implements KvStorageCache {

    private final LoadingCache<String, Optional<KvStorage>> _cache;
    private final AccessChecker _accessChecker;

    KvStorageCacheImpl(LoadingCache<String, Optional<KvStorage>> cache, AccessChecker accessChecker) {
        this._cache = cache;
        _accessChecker = accessChecker;
    }

    public Optional<KvStorage> get(String id) throws ExecutionException {
        Optional<KvStorage> cachedStorage = _cache.get(id);
        if (cachedStorage.isPresent()) {
            KvStorage storage = cachedStorage.get();
            try {
                _accessChecker.check(storage);
            } catch (InvalidEntityException e) {
                return Optional.empty();
            }
        }
        return cachedStorage;
    }

    @Override
    public void invalidateAll(Iterable<String> ids) {
        _cache.invalidateAll(ids);
    }

    @Override
    public void invalidateAll() {
        _cache.invalidateAll();
    }
}
