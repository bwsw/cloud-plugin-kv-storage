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

public class DeleteStorageRequest {

    private final DeleteRequest registryRequest;
    private final DeleteIndexRequest storageIndexRequest;
    private final DeleteIndexRequest historyIndexRequest;

    public DeleteStorageRequest(DeleteRequest registryRequest, DeleteIndexRequest storageIndexRequest, DeleteIndexRequest historyIndexRequest) {
        if (registryRequest == null) {
            throw new IllegalArgumentException("Null storage registry request");
        }
        if (storageIndexRequest == null) {
            throw new IllegalArgumentException("Null storage index request");
        }
        this.registryRequest = registryRequest;
        this.storageIndexRequest = storageIndexRequest;
        this.historyIndexRequest = historyIndexRequest;
    }

    public DeleteRequest getRegistryRequest() {
        return registryRequest;
    }

    public DeleteIndexRequest getStorageIndexRequest() {
        return storageIndexRequest;
    }

    public DeleteIndexRequest getHistoryIndexRequest() {
        return historyIndexRequest;
    }
}
