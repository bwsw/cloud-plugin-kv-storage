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

package com.bwsw.cloudstack.storage.kv.entity;

import com.bwsw.cloudstack.storage.kv.job.JobType;

import java.time.Instant;

public class Lock {

    private final String id;
    private final boolean locked;
    private final long timestamp;

    private Lock(JobType jobType, boolean locked, long timestamp) {
        this.id = jobType.name();
        this.locked = locked;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public boolean isLocked() {
        return locked;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static Lock getAcquiredLock(JobType jobType) {
        return new Lock(jobType, true, Instant.now().toEpochMilli());
    }

    public static Lock getReleasedLock(JobType jobType) {
        return new Lock(jobType, false, 0);
    }
}
