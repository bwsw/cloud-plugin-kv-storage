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
import com.bwsw.cloudstack.storage.kv.entity.EntityConstants;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KvRequestBuilderImpl implements KvRequestBuilder {

    public static final String STORAGE_REGISTRY_INDEX = "storage-registry";
    public static final String STORAGE_TYPE = "_doc";
    public static final String STORAGE_INDEX_PREFIX = "storage-data-";
    public static final String HISTORY_INDEX_PREFIX = "storage-history-";
    private static final String ID_FIELD = "_id";
    private static final String ACCOUNT_FIELD = "account";
    private static final String TYPE_FIELD = "type";
    private static final String MARK_DELETED_STORAGE_SCRIPT = "ctx._source." + EntityConstants.DELETED + " = true; ctx._source." + EntityConstants.LAST_UPDATED + " = ctx._now";
    private static final String UPDATE_TTL_SCRIPT =
            "ctx._source." + EntityConstants.TTL + " = params.ttl; ctx._source." + EntityConstants.EXPIRATION_TIMESTAMP + " = params.expiration_timestamp; ctx._source."
                    + EntityConstants.LAST_UPDATED + " = ctx._now";
    private static final String UPDATE_SECRET_KEY_SCRIPT =
            "ctx._source." + EntityConstants.SECRET_KEY + " = params.secret_key; ctx._source." + EntityConstants.LAST_UPDATED + " = ctx._now";

    private static final ObjectMapper s_objectMapper = new ObjectMapper();
    private static final String SCRIPT_LANG = "painless";
    private static final String LAST_UPDATED_PIPELINE = "storage-registry-last-updated";

    @Override
    public GetRequest getGetRequest(String storageId) {
        GetRequest request = new GetRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storageId);
        return request;
    }

    @Override
    public CreateStorageRequest getCreateRequest(KvStorage storage) throws JsonProcessingException {
        IndexRequest registryRequest = getIndexRequest(storage, DocWriteRequest.OpType.CREATE);
        CreateIndexRequest storageIndexRequest = new CreateIndexRequest(getStorageIndex(storage));
        CreateIndexRequest historyIndexRequest = null;
        if (storage.getHistoryEnabled() != null && storage.getHistoryEnabled()) {
            historyIndexRequest = new CreateIndexRequest(getHistoryIndex(storage));
        }
        return new CreateStorageRequest(registryRequest, storageIndexRequest, historyIndexRequest);
    }

    @Override
    public UpdateRequest getUpdateTTLRequest(KvStorage storage) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("ttl", storage.getTtl());
        parameters.put(EntityConstants.EXPIRATION_TIMESTAMP, storage.getExpirationTimestamp());

        Script script = new Script(ScriptType.INLINE, SCRIPT_LANG, UPDATE_TTL_SCRIPT, parameters);

        return new UpdateRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId()).script(script);
    }

    @Override
    public UpdateRequest getUpdateSecretKey(KvStorage storage) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(EntityConstants.SECRET_KEY, storage.getSecretKey());

        Script script = new Script(ScriptType.INLINE, "painless", UPDATE_SECRET_KEY_SCRIPT, parameters);

        return new UpdateRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId()).script(script);
    }

    @Override
    public SearchRequest getSearchRequest(String accountUuid, int from, int size) {
        SearchRequest searchRequest = new SearchRequest(STORAGE_REGISTRY_INDEX);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(from);
        sourceBuilder.size(size);

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
        return getSearchRequest(size, scrollTimeout, QueryBuilders.termQuery(EntityConstants.DELETED, true));
    }

    @Override
    public SearchRequest getVmStoragesRequest(int size, int scrollTimeout) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.VM.toString()));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));

        return getSearchRequest(size, scrollTimeout, queryBuilder);
    }

    @Override
    public SearchRequest getAccountStoragesRequest(int size, int scrollTimeout) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.ACCOUNT.toString()));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));

        return getSearchRequest(size, scrollTimeout, queryBuilder);
    }

    @Override
    public SearchRequest getAccountStoragesRequest(String accountUuid, int size, int scrollTimeout) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.ACCOUNT.toString()));
        queryBuilder.filter(QueryBuilders.termQuery(ACCOUNT_FIELD, accountUuid));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));

        return getSearchRequest(size, scrollTimeout, queryBuilder);
    }

    @Override
    public SearchRequest getLastUpdatedStoragesRequest(long lastUpdated, int size, int scrollTimeout) {
        SearchRequest request = getSearchRequest(size, scrollTimeout, QueryBuilders.rangeQuery(EntityConstants.LAST_UPDATED).gte(lastUpdated));
        request.source().fetchSource(new String[] {ID_FIELD}, null);
        return request;
    }

    @Override
    public SearchScrollRequest getScrollRequest(String scrollId, int scrollTimeout) {
        SearchScrollRequest request = new SearchScrollRequest(scrollId);
        request.scroll(TimeValue.timeValueMillis(scrollTimeout));
        return request;
    }

    @Override
    public DeleteStorageRequest getDeleteRequest(KvStorage storage) {
        UpdateRequest registryUpdateRequest = getMarkDeletedRequest(storage);
        DeleteRequest registryDeleteRequest = new DeleteRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId());
        DeleteIndexRequest storageIndexRequest = new DeleteIndexRequest(getStorageIndex(storage));
        DeleteIndexRequest historyIndexRequest = null;
        if (storage.getHistoryEnabled() != null && storage.getHistoryEnabled()) {
            historyIndexRequest = new DeleteIndexRequest(getHistoryIndex(storage));
        }
        return new DeleteStorageRequest(registryUpdateRequest, registryDeleteRequest, storageIndexRequest, historyIndexRequest);
    }

    @Override
    public UpdateRequest getMarkDeletedRequest(KvStorage storage) {
        return new UpdateRequest(STORAGE_REGISTRY_INDEX, STORAGE_TYPE, storage.getId()).script(getMarkDeletedScript());
    }

    @Override
    public Request getExpireTempStorageRequest(long timestamp) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.TEMP.name()));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));
        queryBuilder.filter(QueryBuilders.rangeQuery(EntityConstants.EXPIRATION_TIMESTAMP).lte(timestamp));
        return getMarkDeletedRequest(queryBuilder);
    }

    @Override
    public Request getMarkDeletedAccountStorageRequest(List<String> accountUuids) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.ACCOUNT.name()));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));
        queryBuilder.filter(QueryBuilders.termsQuery(ACCOUNT_FIELD, accountUuids));
        return getMarkDeletedRequest(queryBuilder);
    }

    @Override
    public Request getMarkDeletedVmStorageRequest(List<String> vmUuids) throws IOException {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.termQuery(TYPE_FIELD, KvStorage.KvStorageType.VM.name()));
        queryBuilder.filter(QueryBuilders.termQuery(EntityConstants.DELETED, false));
        queryBuilder.filter(QueryBuilders.termsQuery(ID_FIELD, vmUuids));
        return getMarkDeletedRequest(queryBuilder);
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

    private SearchRequest getSearchRequest(int size, int scrollTimeout, QueryBuilder queryBuilder) {
        SearchRequest searchRequest = new SearchRequest(STORAGE_REGISTRY_INDEX);
        searchRequest.scroll(TimeValue.timeValueMillis(scrollTimeout));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(size);
        sourceBuilder.query(queryBuilder);

        searchRequest.source(sourceBuilder);
        return searchRequest;

    }

    private Request getMarkDeletedRequest(QueryBuilder queryBuilder) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("conflicts", "proceed");

        Script script = getMarkDeletedScript();

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        contentBuilder.field("script");
        script.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.field("query");
        queryBuilder.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        StringEntity entity = new StringEntity(contentBuilder.string(), ContentType.APPLICATION_JSON);

        return new Request("POST", STORAGE_REGISTRY_INDEX + "/_update_by_query?pipeline=" + LAST_UPDATED_PIPELINE, params, entity);
    }

    private Script getMarkDeletedScript() {
        return new Script(ScriptType.INLINE, "painless", MARK_DELETED_STORAGE_SCRIPT, Collections.emptyMap());
    }
}
