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

import com.bwsw.cloudstack.storage.kv.client.KvStorageClientManager;
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.utils.component.ComponentLifecycleBase;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KvStorageJobManagerImpl extends ComponentLifecycleBase implements KvStorageJobManager {

    private static final long AWAIT_TERMINATION_DELAY = 30000;

    private static final Logger s_logger = Logger.getLogger(KvStorageJobManagerImpl.class);

    private final ScheduledExecutorService _executor = Executors.newScheduledThreadPool(JobType.values().length);

    @Inject
    private KvStorageLockManager _kvStorageLockManager;

    @Inject
    private KvStorageManager _kvStorageManager;

    @Inject
    private KvStorageClientManager _kvStorageClientManager;

    @Override
    public boolean configure(String name, Map<String, Object> params) {
        return true;
    }

    @Override
    public boolean start() {
        for (JobType jobType : JobType.values()) {
            Runnable job = getJob(jobType);
            if (job != null) {
                _executor.scheduleAtFixedRate(job, 0, jobType.getInterval(), TimeUnit.MILLISECONDS);
            }
        }
        return true;
    }

    @Override
    public boolean stop() {
        _executor.shutdownNow();
        try {
            _executor.awaitTermination(AWAIT_TERMINATION_DELAY, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            s_logger.error("Exception while stopping KV job manager", e);
        }
        return true;
    }

    @Override
    public Runnable getJob(JobType jobType) {
        switch (jobType) {
        case TEMP_STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, _kvStorageClientManager) {

                @Override
                protected void doJob() {
                    _kvStorageManager.expireTempStorages();
                }
            };
        case STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, _kvStorageClientManager) {
                @Override
                protected void doJob() {
                    _kvStorageManager.cleanupStorages();
                }
            };
        case VM_ACCOUNT_RECENTLY_DELETED_STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, _kvStorageClientManager) {
                @Override
                protected void doJob() {
                    int interval = jobType.getInterval() * 2;
                    _kvStorageManager.deleteAccountStoragesForRecentlyDeletedAccount(interval);
                    _kvStorageManager.deleteVmStoragesForRecentlyDeletedVms(interval);
                }
            };
        case VM_ACCOUNT_STORAGE_CLEANUP:
            return new JobRunnable(jobType, _kvStorageLockManager, _kvStorageClientManager) {
                @Override
                protected void doJob() {
                    _kvStorageManager.deleteExpungedVmStorages();
                    _kvStorageManager.deleteAccountStoragesForDeletedAccounts();
                }
            };
        }
        return null;
    }
}
