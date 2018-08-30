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

import com.bwsw.cloudstack.storage.kv.client.KvStorageClientManager;
import com.bwsw.cloudstack.storage.kv.entity.ScrollableListResponse;
import com.bwsw.cloudstack.storage.kv.service.KvExecutor;
import com.bwsw.cloudstack.storage.kv.service.KvRequestBuilder;
import com.bwsw.cloudstack.storage.kv.util.TimeManager;
import com.cloud.utils.component.ComponentLifecycleBase;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;

import javax.inject.Inject;
import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class KvStorageCacheUpdater extends ComponentLifecycleBase {

    private static final Logger s_logger = Logger.getLogger(KvStorageCacheUpdater.class);

    private static final int UPDATE_BATCH_SIZE = 1000;
    private static final int UPDATE_BATCH_TIMEOUT = 60000; // 1 minute
    private static final int UPDATE_PERIOD = 60000; // 1 minute

    private class KvStorageCacheUpdateTask extends TimerTask {

        private long lastUpdated = _timeManager.getCurrentTimestamp();

        @Override
        public void run() {
            s_logger.info("Update of KV storage cache started");
            long startTimestamp = _timeManager.getCurrentTimestamp();
            SearchRequest request = _kvRequestBuilder.getLastUpdatedStoragesRequest(lastUpdated - UPDATE_PERIOD, UPDATE_BATCH_SIZE, UPDATE_BATCH_TIMEOUT);
            try {
                ScrollableListResponse<String> response = _kvExecutor.scrollIds(_kvStorageClientManager.getEsClient(), request);
                while (response != null && response.getResults() != null && !response.getResults().isEmpty()) {
                    _kvStorageCache.invalidateAll(response.getResults());
                    response = _kvExecutor.scrollIds(_kvStorageClientManager.getEsClient(), _kvRequestBuilder.getScrollRequest(response.getScrollId(), UPDATE_BATCH_TIMEOUT));
                }
                lastUpdated = startTimestamp;
            } catch (IOException e) {
                s_logger.error("Unable to update KV storage cache", e);
                _kvStorageCache.invalidateAll();
            }
            s_logger.info("Update of KV storage cache finish");
        }
    }

    @Inject
    private KvRequestBuilder _kvRequestBuilder;

    @Inject
    private KvExecutor _kvExecutor;

    @Inject
    private KvStorageCache _kvStorageCache;

    @Inject
    private KvStorageClientManager _kvStorageClientManager;

    @Inject
    private TimeManager _timeManager;

    private Timer _timer;

    @Override
    public boolean configure(String name, Map<String, Object> params) throws ConfigurationException {
        super.configure(name, params);
        _timer = new Timer("KvStorageCacheUpdater");
        return true;
    }

    @Override
    public boolean start() {
        _timer.schedule(new KvStorageCacheUpdateTask(), UPDATE_PERIOD, UPDATE_PERIOD);
        return true;
    }

    @Override
    public boolean stop() {
        _timer.cancel();
        return true;
    }
}
