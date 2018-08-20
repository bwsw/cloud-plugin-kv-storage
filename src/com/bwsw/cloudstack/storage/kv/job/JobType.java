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

public enum JobType {

    TEMP_STORAGE_CLEANUP(60000), STORAGE_CLEANUP(3600000), VM_ACCOUNT_RECENTLY_DELETED_STORAGE_CLEANUP(300000), VM_ACCOUNT_STORAGE_CLEANUP(86400000);

    // _interval in ms
    private final int _interval;

    JobType(int interval) {
        this._interval = interval;
    }

    public int getInterval() {
        return _interval;
    }

}
