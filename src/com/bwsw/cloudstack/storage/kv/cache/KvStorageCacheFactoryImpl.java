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
import com.bwsw.cloudstack.storage.kv.service.KvExecutor;
import com.bwsw.cloudstack.storage.kv.service.KvRequestBuilder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.RestHighLevelClient;

import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class KvStorageCacheFactoryImpl implements KvStorageCacheFactory {

    @Inject
    private AccessChecker _accessChecker;

    @Inject
    private KvRequestBuilder _kvRequestBuilder;

    @Inject
    private KvExecutor _kvExecutor;

    @Override
    public KvStorageCache getCache(int maxSize, RestHighLevelClient restHighLevelClient) {
        LoadingCache<String, Optional<KvStorage>> cache = CacheBuilder.newBuilder().maximumSize(maxSize).expireAfterAccess(1, TimeUnit.HOURS).refreshAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Optional<KvStorage>>() {
                    @Override
                    public Optional<KvStorage> load(String key) throws Exception {
                        GetRequest request = _kvRequestBuilder.getGetRequest(key);
                        KvStorage storage = _kvExecutor.get(restHighLevelClient, request, KvStorage.class);
                        if (storage == null) {
                            return Optional.empty();
                        }
                        if (storage.getType() == null || storage.getDeleted() == null) {
                            throw new InvalidEntityException();
                        }
                        if (storage.getDeleted()) {
                            return Optional.empty();
                        }
                        return Optional.of(storage);
                    }
                });
        return new KvStorageCacheImpl(cache, _accessChecker);
    }
}
