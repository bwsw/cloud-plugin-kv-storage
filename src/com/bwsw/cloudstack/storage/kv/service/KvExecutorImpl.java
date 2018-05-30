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

import com.bwsw.cloudstack.storage.kv.entity.ResponseEntity;
import com.cloud.utils.exception.CloudRuntimeException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudstack.api.response.ListResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KvExecutorImpl implements KvExecutor {

    private final ObjectMapper _objectMapper = new ObjectMapper();

    @Override
    public void index(RestHighLevelClient client, IndexRequest request) throws IOException {
        IndexResponse response = client.index(request);
        if (response.status() != RestStatus.CREATED || response.status() != RestStatus.OK) {
            throw new CloudRuntimeException("Failed to execute create/update operation");
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

    private <T extends ResponseEntity> List<T> parseResults(SearchResponse response, Class<T> elementClass) throws IOException {
        List<T> results = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            T result = _objectMapper.readValue(searchHit.getSourceAsString(), elementClass);
            result.setId(searchHit.getId());
            results.add(result);
        }
        return results;
    }
}
