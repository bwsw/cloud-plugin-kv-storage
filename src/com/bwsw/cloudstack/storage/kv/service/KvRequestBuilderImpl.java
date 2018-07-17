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
import com.bwsw.cloudstack.storage.kv.entity.EntityConstants;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KvRequestBuilderImpl implements KvRequestBuilder {

    public static final String STORAGE_REGISTRY_INDEX = "storage-registry";
    public static final String STORAGE_TYPE = "_doc";
    public static final String STORAGE_INDEX_PREFIX = "storage-data-";
    public static final String HISTORY_INDEX_PREFIX = "storage-history-";
    private static final String ID_FIELD = "_id";
    private static final String ACCOUNT_FIELD = "account";
    private static final String TYPE_FIELD = "type";
    private static final String NAME_FIELD = "name";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String[] FIELDS = {ID_FIELD, TYPE_FIELD, NAME_FIELD, DESCRIPTION_FIELD, EntityConstants.HISTORY_ENABLED, EntityConstants.DELETED};
    private static final String EXPIRE_TEMP_STORAGE_SCRIPT = "ctx._source.deleted = true";

    private static final ObjectMapper s_objectMapper = new ObjectMapper();

    @Override
    public GetRequest getGetRequest(String storageId) {
        GetRequest request = new GetRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storageId);
        return request;
    }

    @Override
    public IndexRequest getCreateRequest(KvStorage storage) throws JsonProcessingException {
        return getIndexRequest(storage, DocWriteRequest.OpType.CREATE);
    }

    @Override
    public IndexRequest getUpdateRequest(KvStorage storage) throws JsonProcessingException {
        return getIndexRequest(storage, DocWriteRequest.OpType.INDEX);
    }

    @Override
    public UpdateRequest getUpdateTTLRequest(KvStorage storage) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("ttl", storage.getTtl());
        parameters.put(EntityConstants.EXPIRATION_TIMESTAMP, storage.getExpirationTimestamp());
        return new UpdateRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId()).doc(parameters);
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
    public SearchRequest getDeletedStoragesRequest(int size, int scrollTimeout) {
        SearchRequest searchRequest = new SearchRequest(STORAGE_REGISTRY_INDEX);
        searchRequest.scroll(TimeValue.timeValueMillis(scrollTimeout));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(size);
        sourceBuilder.fetchSource(FIELDS, null);
        sourceBuilder.query(QueryBuilders.termQuery(EntityConstants.DELETED, true));

        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    @Override
    public SearchScrollRequest getScrollRequest(String scrollId, int scrollTimeout) {
        SearchScrollRequest request = new SearchScrollRequest(scrollId);
        request.scroll(TimeValue.timeValueMillis(scrollTimeout));
        return request;
    }

    @Override
    public DeleteStorageRequest getDeleteRequest(KvStorage storage) throws JsonProcessingException {
        IndexRequest registryUpdateRequest = getUpdateRequest(storage);
        DeleteRequest registryDeleteRequest = new DeleteRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId());
        DeleteIndexRequest storageIndexRequest = new DeleteIndexRequest(getStorageIndex(storage));
        DeleteIndexRequest historyIndexRequest = null;
        if (storage.getHistoryEnabled() != null && storage.getHistoryEnabled()) {
            historyIndexRequest = new DeleteIndexRequest(getHistoryIndex(storage));
        }
        return new DeleteStorageRequest(registryUpdateRequest, registryDeleteRequest, storageIndexRequest, historyIndexRequest);
    }

    @Override
    public Request getExpireTempStorageRequest(long timestamp) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("conflicts", "proceed");

        Script script = new Script(ScriptType.INLINE, "painless", EXPIRE_TEMP_STORAGE_SCRIPT, Collections.emptyMap());

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery("type", KvStorage.KvStorageType.TEMP.name()));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));
        queryBuilder.filter(QueryBuilders.rangeQuery(EntityConstants.EXPIRATION_TIMESTAMP).lte(timestamp));

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        contentBuilder.field("script");
        script.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.field("query");
        queryBuilder.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        StringEntity entity = new StringEntity(contentBuilder.string(), ContentType.APPLICATION_JSON);

        return new Request("POST", STORAGE_REGISTRY_INDEX + "/_update_by_query", params, entity);
    }

    private String getStorageIndex(KvStorage storage) {
        return STORAGE_INDEX_PREFIX + storage.getId();
    }

    private String getHistoryIndex(KvStorage storage) {
        return HISTORY_INDEX_PREFIX + storage.getId();
    }

    private IndexRequest getIndexRequest(KvStorage storage, DocWriteRequest.OpType opType) throws JsonProcessingException {
        IndexRequest request = new IndexRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId());
        request.source(s_objectMapper.writeValueAsString(storage), XContentType.JSON);
        request.opType(opType);
        return request;
    }
}
