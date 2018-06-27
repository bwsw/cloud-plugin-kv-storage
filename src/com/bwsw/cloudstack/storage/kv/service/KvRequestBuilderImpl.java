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

import com.bwsw.cloudstack.storage.kv.entity.DeleteStorageRequest;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class KvRequestBuilderImpl implements KvRequestBuilder {

    public static final String STORAGE_REGISTRY_INDEX = "storage-registry";
    public static final String STORAGE_TYPE = "_doc";
    public static final String STORAGE_INDEX_PREFIX = "storage-";
    public static final String HISTORY_INDEX_PREFIX = "history-storage-";
    private static final String ID_FIELD = "_id";
    private static final String ACCOUNT_FIELD = "account";
    private static final String TYPE_FIELD = "type";
    private static final String NAME_FIELD = "name";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String[] FIELDS = {ID_FIELD, NAME_FIELD, DESCRIPTION_FIELD};

    private static final ObjectMapper s_objectMapper = new ObjectMapper();

    @Override
    public GetRequest getGetRequest(String storageId) {
        GetRequest request = new GetRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storageId);
        return request;
    }

    @Override
    public IndexRequest getCreateRequest(KvStorage storage) throws JsonProcessingException {
        IndexRequest request = new IndexRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId());
        request.source(s_objectMapper.writeValueAsString(storage), XContentType.JSON);
        request.opType(DocWriteRequest.OpType.CREATE);
        return request;
    }

    @Override
    public SearchRequest getSearchRequest(String accountUuid, int from, int size) {
        SearchRequest searchRequest = new SearchRequest(STORAGE_REGISTRY_INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(from);
        sourceBuilder.size(size);

        sourceBuilder.fetchSource(FIELDS, null);

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(ACCOUNT_FIELD, accountUuid));
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.ACCOUNT.toString()));
        sourceBuilder.query(queryBuilder);

        sourceBuilder.sort(new FieldSortBuilder(ID_FIELD).order(SortOrder.ASC));

        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    @Override
    public DeleteStorageRequest getDeleteRequest(KvStorage storage) {
        DeleteRequest registryRequest = new DeleteRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId());
        DeleteIndexRequest storageIndexRequest = new DeleteIndexRequest(getStorageIndex(storage));
        DeleteIndexRequest historyIndexRequest = null;
        if (storage.getHistoryEnabled() != null && storage.getHistoryEnabled()) {
            historyIndexRequest = new DeleteIndexRequest(getHistoryIndex(storage));
        }
        return new DeleteStorageRequest(registryRequest, storageIndexRequest, historyIndexRequest);
    }

    private String getStorageIndex(KvStorage storage) {
        return STORAGE_INDEX_PREFIX + storage.getId();
    }

    private String getHistoryIndex(KvStorage storage) {
        return HISTORY_INDEX_PREFIX + storage.getId();
    }
}
