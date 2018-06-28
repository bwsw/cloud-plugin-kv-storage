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

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;

public class DeleteStorageRequest {

    private final IndexRequest registryUpdateRequest;
    private final DeleteRequest registryDeleteRequest;
    private final DeleteIndexRequest storageIndexRequest;
    private final DeleteIndexRequest historyIndexRequest;

    public DeleteStorageRequest(IndexRequest registryUpdateRequest, DeleteRequest registryDeleteRequest, DeleteIndexRequest storageIndexRequest, DeleteIndexRequest historyIndexRequest) {
        if (registryUpdateRequest == null) {
            throw new IllegalArgumentException("Null registry update request");
        }
        if (registryDeleteRequest == null) {
            throw new IllegalArgumentException("Null registry delete request");
        }
        if (storageIndexRequest == null) {
            throw new IllegalArgumentException("Null storage index request");
        }
        this.registryUpdateRequest = registryUpdateRequest;
        this.registryDeleteRequest = registryDeleteRequest;
        this.storageIndexRequest = storageIndexRequest;
        this.historyIndexRequest = historyIndexRequest;
    }

    public IndexRequest getRegistryUpdateRequest() {
        return registryUpdateRequest;
    }

    public DeleteRequest getRegistryDeleteRequest() {
        return registryDeleteRequest;
    }

    public DeleteIndexRequest getStorageIndexRequest() {
        return storageIndexRequest;
    }

    public DeleteIndexRequest getHistoryIndexRequest() {
        return historyIndexRequest;
    }
}
