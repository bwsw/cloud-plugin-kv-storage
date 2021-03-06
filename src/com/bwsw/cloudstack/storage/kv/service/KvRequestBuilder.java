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

import com.bwsw.cloudstack.storage.kv.entity.CreateStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.DeleteStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;

import java.io.IOException;
import java.util.List;

public interface KvRequestBuilder {

    GetRequest getGetRequest(String storageId);

    CreateStorageRequest getCreateRequest(KvStorage storage) throws JsonProcessingException;

    UpdateRequest getUpdateTTLRequest(KvStorage storage);

    UpdateRequest getUpdateSecretKey(KvStorage storage);

    SearchRequest getSearchRequest(String accountUuid, int from, int size);

    SearchRequest getDeletedStoragesRequest(int size, int scrollTimeout);

    SearchRequest getVmStoragesRequest(int size, int scrollTimeout);

    SearchRequest getAccountStoragesRequest(int size, int scrollTimeout);

    SearchRequest getAccountStoragesRequest(String accountUuid, int size, int scrollTimeout);

    SearchRequest getLastUpdatedStoragesRequest(long lastUpdated, int size, int scrollTimeout);

    SearchScrollRequest getScrollRequest(String scrollId, int scrollTimeout);

    DeleteStorageRequest getDeleteRequest(KvStorage storage) throws JsonProcessingException;

    UpdateRequest getMarkDeletedRequest(KvStorage storage);

    Request getExpireTempStorageRequest(long timestamp) throws IOException;

    Request getMarkDeletedAccountStorageRequest(List<String> accountUuids) throws IOException;

    Request getMarkDeletedVmStorageRequest(List<String> vmUuids) throws IOException;
}
