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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.function.Consumer;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KvStorageJobManagerImplTest {

    @Mock
    private KvStorageLockManager _kvStorageLockManager;

    @Mock
    private KvStorageManager _kvStorageManager;

    @Mock
    private RestHighLevelClient _restHighLevelClient;

    @InjectMocks
    private KvStorageJobManagerImpl _kvStorageJobManager = new KvStorageJobManagerImpl();

    @Test
    public void testGetJobTempStorageCleanup() {
        testGetJob(JobType.TEMP_STORAGE_CLEANUP, manager -> doNothing().when(manager).expireTempStorages(), manager -> verify(manager).expireTempStorages());
    }

    @Test
    public void testGetJobStorageCleanup() {
        testGetJob(JobType.STORAGE_CLEANUP, manager -> doNothing().when(manager).cleanupStorages(), manager -> verify(manager).cleanupStorages());
    }

    private void testGetJob(JobType jobType, Consumer<KvStorageManager> expectSetter, Consumer<KvStorageManager> verifier) {
        Runnable job = _kvStorageJobManager.getJob(jobType, _kvStorageManager, _restHighLevelClient);

        assertNotNull(job);

        when(_kvStorageLockManager.acquireLock(jobType, _restHighLevelClient)).thenReturn(true);
        expectSetter.accept(_kvStorageManager);
        doNothing().when(_kvStorageLockManager).releaseLock(jobType, _restHighLevelClient);
        job.run();
        verifier.accept(_kvStorageManager);
    }
}
