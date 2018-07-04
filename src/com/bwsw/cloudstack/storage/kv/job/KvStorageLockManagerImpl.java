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

package com.bwsw.cloudstack.storage.kv.job;

import com.bwsw.cloudstack.storage.kv.entity.Lock;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KvStorageLockManagerImpl implements KvStorageLockManager {

    private static final Logger s_logger = Logger.getLogger(KvStorageLockManagerImpl.class);

    private static final String LOCK_INDEX = "registry-storage-lock";
    private static final String LOCK_TYPE = "_doc";
    private static final String ACQUIRE_SCRIPT = "if (ctx._source.locked == false || params.timestamp - ctx._source.timestamp > params.interval ) "
            + "{ ctx._source.locked = true; ctx._source.timestamp = params.timestamp } else { ctx.op='noop'}";
    private static final String RELEASE_SCRIPT = "ctx._source.locked = false; ctx._source.timestamp = params.timestamp";

    @Override
    public boolean acquireLock(JobType jobType, RestHighLevelClient client) {
        Lock lock = Lock.getAcquiredLock(jobType);

        Map<String, Object> params = new HashMap<>();
        params.put("timestamp", lock.getTimestamp());
        params.put("interval", jobType.getInterval());

        UpdateRequest request = getUpdateRequest(lock.getId(), ACQUIRE_SCRIPT, params);
        request.upsert(getDocument(lock));

        try {
            UpdateResponse response = client.update(request);
            return (response.status() == RestStatus.OK && response.getResult() == DocWriteResponse.Result.UPDATED) || (response.status() == RestStatus.CREATED
                    && response.getResult() == DocWriteResponse.Result.CREATED);
        } catch (IOException | ElasticsearchException e) {
            s_logger.error("Unable to acquire the lock " + lock.getId(), e);
            return false;
        }
    }

    @Override
    public void releaseLock(JobType jobType, RestHighLevelClient client) {
        Lock lock = Lock.getReleasedLock(jobType);

        Map<String, Object> params = new HashMap<>();
        params.put("timestamp", lock.getTimestamp());

        UpdateRequest request = getUpdateRequest(lock.getId(), RELEASE_SCRIPT, params);
        try {
            client.update(request);
        } catch (IOException e) {
            s_logger.error("Unable to release the lock " + lock.getId(), e);
        }
    }

    private UpdateRequest getUpdateRequest(String id, String script, Map<String, Object> params) {
        UpdateRequest request = new UpdateRequest(LOCK_INDEX, LOCK_TYPE, id);
        request.script(new Script(ScriptType.INLINE, "painless", script, params));
        return request;
    }

    private Map<String, Object> getDocument(Lock lock) {
        Map<String, Object> document = new HashMap<>();
        document.put("locked", lock.isLocked());
        document.put("timestamp", lock.getTimestamp());
        return document;
    }
}
