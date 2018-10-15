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

package com.bwsw.cloudstack.storage.kv.service;

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.response.KvHistoryResult;
import com.bwsw.cloudstack.storage.kv.response.KvKey;
import com.bwsw.cloudstack.storage.kv.response.KvKeys;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import com.bwsw.cloudstack.storage.kv.response.KvPair;
import com.bwsw.cloudstack.storage.kv.response.KvResult;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface KvOperationManager {

    KvOperationResponse get(KvStorage storage, String key);

    KvOperationResponse get(KvStorage storage, Collection<String> keys);

    KvPair set(KvStorage storage, String key, String value);

    KvResult set(KvStorage storage, Map<String, String> data);

    KvKey delete(KvStorage storage, String key);

    KvResult delete(KvStorage storage, Collection<String> keys);

    KvKeys list(KvStorage storage);

    KvOperationResponse clear(KvStorage storage);

    KvHistoryResult getHistory(KvStorage storage, List<String> keys, List<String> operations, Long start, Long end, List<String> sort, Integer page, Integer size, Long scroll);

}
