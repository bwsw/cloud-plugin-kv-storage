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
import com.bwsw.cloudstack.storage.kv.entity.ResponseEntity;
import com.bwsw.cloudstack.storage.kv.entity.ScrollableListResponse;
import com.cloud.utils.exception.CloudRuntimeException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudstack.api.response.ListResponse;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KvExecutorImpl implements KvExecutor {

    private static final Logger s_logger = Logger.getLogger(KvExecutorImpl.class);

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @Override
    public <T extends ResponseEntity> T get(RestHighLevelClient client, GetRequest request, Class<T> elementClass) throws IOException {
        GetResponse response = client.get(request);
        if (!response.isExists()) {
            return null;
        }
        if (response.isSourceEmpty()) {
            throw new CloudRuntimeException("Empty result for get operation");
        }
        return parseResult(response.getSourceAsString(), elementClass, response.getId());
    }

    @Override
    public void index(RestHighLevelClient client, IndexRequest request) throws IOException {
        IndexResponse response = client.index(request);
        if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
            throw new CloudRuntimeException("Failed to execute create/update operation");
        }
    }

    @Override
    public void update(RestHighLevelClient client, UpdateRequest request) throws IOException {
        UpdateResponse response = client.update(request);
        if (response.status() != RestStatus.OK) {
            throw new CloudRuntimeException("Failed to execute update operation");
        }
    }

    @Override
    public <T extends ResponseEntity> ListResponse<T> search(RestHighLevelClient client, SearchRequest request, Class<T> elementClass) throws IOException {
        SearchResponse response = client.search(request);
        if (response.status() != RestStatus.OK || response.getHits() == null) {
            throw new CloudRuntimeException("Failed to execute search operation");
        }
        ListResponse<T> results = new ListResponse<>();
        results.setResponses(parseResults(response, elementClass), (int)response.getHits().getTotalHits());
        return results;
    }

    @Override
    public <T extends ResponseEntity> ScrollableListResponse<T> scroll(RestHighLevelClient client, SearchRequest request, Class<T> elementClass) throws IOException {
        return parseScroll(client.search(request), elementClass);
    }

    @Override
    public <T extends ResponseEntity> ScrollableListResponse<T> scroll(RestHighLevelClient client, SearchScrollRequest request, Class<T> elementClass) throws IOException {
        return parseScroll(client.searchScroll(request), elementClass);
    }

    @Override
    public ScrollableListResponse<String> scrollIds(RestHighLevelClient client, SearchRequest request) throws IOException {
        return parseScrollIds(client.search(request));
    }

    @Override
    public ScrollableListResponse<String> scrollIds(RestHighLevelClient client, SearchScrollRequest request) throws IOException {
        return parseScrollIds(client.searchScroll(request));
    }

    @Override
    public void create(RestHighLevelClient client, CreateStorageRequest request) throws IOException {
        index(client, request.getRegistryRequest());
        createIndex(client, request.getStorageIndexRequest());
        if (request.getHistoryIndexRequest() != null) {
            createIndex(client, request.getHistoryIndexRequest());
        }
    }

    @Override
    public boolean delete(RestHighLevelClient client, DeleteStorageRequest request) throws IOException {
        UpdateResponse registryUpdateResponse = client.update(request.getRegistryUpdateRequest());
        if (registryUpdateResponse.status() != RestStatus.OK) {
            return false;
        }
        if (!deleteIndex(client, request.getStorageIndexRequest())) {
            return false;
        }
        if (request.getHistoryIndexRequest() != null && !deleteIndex(client, request.getHistoryIndexRequest())) {
            return false;
        }
        DeleteResponse registryDeleteResponse = client.delete(request.getRegistryDeleteRequest());
        return registryDeleteResponse.status() == RestStatus.OK || registryDeleteResponse.status() == RestStatus.NOT_FOUND;
    }

    private <T extends ResponseEntity> List<T> parseResults(SearchResponse response, Class<T> elementClass) throws IOException {
        List<T> results = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            results.add(parseResult(searchHit.getSourceAsString(), elementClass, searchHit.getId()));
        }
        return results;
    }

    private <T extends ResponseEntity> ScrollableListResponse<T> parseScroll(SearchResponse response, Class<T> elementClass) throws IOException {
        if (response.status() != RestStatus.OK || response.getHits() == null) {
            throw new CloudRuntimeException("Failed to execute search operation");
        }
        return new ScrollableListResponse<>(response.getScrollId(), parseResults(response, elementClass));
    }

    private ScrollableListResponse<String> parseScrollIds(SearchResponse response) {
        if (response.status() != RestStatus.OK || response.getHits() == null) {
            throw new CloudRuntimeException("Failed to execute search operation");
        }
        List<String> results = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            results.add(searchHit.getId());
        }
        return new ScrollableListResponse<>(response.getScrollId(), results);
    }

    private <T extends ResponseEntity> T parseResult(String source, Class<T> elementClass, String id) throws IOException {
        T result = _objectMapper.readValue(source, elementClass);
        result.setId(id);
        return result;
    }

    private boolean deleteIndex(RestHighLevelClient client, DeleteIndexRequest request) throws IOException {
        try {
            DeleteIndexResponse storageIndexResponse = client.indices().delete(request);
            return storageIndexResponse.isAcknowledged();
        } catch (ElasticsearchException exception) {
            return exception.status() == RestStatus.NOT_FOUND;
        }
    }

    private void createIndex(RestHighLevelClient client, CreateIndexRequest request) {
        try {
            client.indices().create(request);
        } catch (IOException e) {
            s_logger.error("Unabled to create an index: " + request.index(), e);
        }
    }
}
