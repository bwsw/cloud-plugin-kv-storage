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

import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import org.elasticsearch.client.RestHighLevelClient;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KvStorageJobManagerImpl implements KvStorageJobManager {

    private final ScheduledExecutorService _executor = Executors.newScheduledThreadPool(JobType.values().length);

    @Inject
    private KvStorageLockManager _kvStorageLockManager;

    @Override
    public void init(KvStorageManager storageManager, RestHighLevelClient client) {
        for (JobType jobType : JobType.values()) {
            Runnable job = getJob(jobType, storageManager, client);
            if (job != null) {
                _executor.scheduleAtFixedRate(job, 0, jobType.getInterval(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public Runnable getJob(JobType jobType, KvStorageManager storageManager, RestHighLevelClient client) {
        switch (jobType) {
        case TEMP_STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, client) {

                @Override
                protected void doJob() {
                    storageManager.expireTempStorages();
                }
            };
        case STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, client) {
                @Override
                protected void doJob() {
                    storageManager.cleanupStorages();
                }
            };
        case VM_ACCOUNT_STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, client) {
                @Override
                protected void doJob() {
                    int interval = jobType.getInterval() * 2;
                    storageManager.deleteAccountStorageForRecentlyDeletedAccount(interval);
                    storageManager.deleteVmStoragesForRecentlyRemovedVms(interval);
                }
            };
        case ENTITY_RELATED_STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, client) {
                @Override
                protected void doJob() {
                    storageManager.deleteExpungedVmStorages();
                    storageManager.deleteAccountStorageForDeletedAccounts();
                }
            };
        }
        return null;
    }
}
