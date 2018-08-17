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

import org.apache.log4j.Logger;
import org.elasticsearch.client.RestHighLevelClient;

public abstract class JobRunnable implements Runnable {

    private static final Logger s_logger = Logger.getLogger(JobRunnable.class);

    private final JobType _jobType;
    private final KvStorageLockManager _kvStorageLockManager;
    private final RestHighLevelClient _restHighLevelClient;

    protected JobRunnable(JobType jobType, KvStorageLockManager kvStorageLockManager, RestHighLevelClient restHighLevelClient) {
        _jobType = jobType;
        _kvStorageLockManager = kvStorageLockManager;
        _restHighLevelClient = restHighLevelClient;
    }

    @Override
    public void run() {
        s_logger.info("Job " + _jobType.name() + " started");
        if (_kvStorageLockManager.acquireLock(_jobType, _restHighLevelClient)) {
            s_logger.info("Lock " + _jobType.name() + " is acquired");
            try {
                doJob();
            } catch (Exception e) {
                s_logger.error("Exception while executing the job " + _jobType.name(), e);
            }
            s_logger.info("Releasing lock " + _jobType.name());
            _kvStorageLockManager.releaseLock(_jobType, _restHighLevelClient);
        } else {
            s_logger.info("Lock " + _jobType.name() + " is not acquired");
        }
        s_logger.info("Job " + _jobType.name() + " finished");
    }

    protected abstract void doJob();
}
