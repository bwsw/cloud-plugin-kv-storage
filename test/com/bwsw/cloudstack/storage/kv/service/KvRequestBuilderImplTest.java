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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(DataProviderRunner.class)
public class KvRequestBuilderImplTest {

    private static final String UUID = "61d12f36-0201-4035-b6fc-c7f768f583f1";
    private static final String SECRET_KEY = "secretkey";
    private static final List<String> UUID_LIST = ImmutableList.of("40de546f-f418-46df-9bde-b3fb2aebc035", "8d48167c-3bfc-47dd-b152-795a7fab4eff");
    private static final int FROM = 10;
    private static final int SIZE = 5;
    private static final int TTL = 300000;
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static final String SCROLL_ID = "scrollId";
    private static final KvStorage TEMP_STORAGE = new KvStorage(UUID, SECRET_KEY, TTL, TIMESTAMP);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private KvRequestBuilderImpl _kvRequestBuilder = new KvRequestBuilderImpl();

    private ObjectMapper objectMapper = new ObjectMapper();

    @DataProvider
    public static Object[][] storages() {
        return new Object[][] {{get("id val", KvStorage.KvStorageType.ACCOUNT, "account secret", "account val", "name val", "description val", null, null, true, false),
                "{\"type\":\"ACCOUNT\",\"deleted\":false,\"account\":\"account val\",\"name\":\"name val\",\"description\":\"description val\",\"secret_key\":\"account secret\","
                        + "\"history_enabled\":true}"}, {get("id val", KvStorage.KvStorageType.VM, "vm secret", null, null, null, null, null, true, false),
                "{\"type\":\"VM\",\"deleted\":false,\"secret_key\":\"vm secret\",\"history_enabled\":true}"},
                {get("id val", KvStorage.KvStorageType.TEMP, "temp secret", null, null, null, 300000, 1527067849287L, true, false),
                        "{\"type\":\"TEMP\",\"deleted\":false,\"ttl\":300000,\"secret_key\":\"temp secret\",\"history_enabled\":true,\"expiration_timestamp\":1527067849287}"}};
    }

    @Test
    public void testGetGetRequest() {
        GetRequest request = _kvRequestBuilder.getGetRequest(UUID);

        assertNotNull(request);
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX, request.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, request.type());
        assertEquals(UUID, request.id());
    }

    @UseDataProvider("storages")
    @Test
    public void testGetCreateRequest(KvStorage storage, String source) throws JsonProcessingException {
        CreateStorageRequest request = _kvRequestBuilder.getCreateRequest(storage);

        assertNotNull(request);
        IndexRequest registryRequest = request.getRegistryRequest();
        assertNotNull(registryRequest);
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX, registryRequest.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, registryRequest.type());
        assertEquals(DocWriteRequest.OpType.CREATE, registryRequest.opType());
        assertEquals(storage.getId(), registryRequest.id());
        assertEquals(source, registryRequest.source().utf8ToString());

        checkCreateIndexRequest(request.getStorageIndexRequest(), KvRequestBuilderImpl.STORAGE_INDEX_PREFIX + storage.getId());

        if (storage.getHistoryEnabled() != null && storage.getHistoryEnabled()) {
            checkCreateIndexRequest(request.getHistoryIndexRequest(), KvRequestBuilderImpl.HISTORY_INDEX_PREFIX + storage.getId());
        }
    }

    @UseDataProvider("storages")
    @Test
    public void testGetUpdateRequest(KvStorage storage, String source) throws JsonProcessingException {
        IndexRequest request = _kvRequestBuilder.getUpdateRequest(storage);

        checkUpdateRequest(request, storage, source);
    }

    @Test
    public void testGetDeleteRequestHistoryEnabledStorage() throws JsonProcessingException {
        KvStorage storage = new KvStorage(UUID, SECRET_KEY, true);
        storage.setDeleted(true);

        testDeleteStorageRequest(storage, "{\"type\":\"VM\",\"deleted\":true,\"secret_key\":\"secretkey\",\"history_enabled\":true}");
    }

    @Test
    public void testGetDeleteRequestHistoryDisabledStorage() throws JsonProcessingException {
        KvStorage storage = new KvStorage(UUID, SECRET_KEY, false);
        storage.setDeleted(true);

        testDeleteStorageRequest(storage, "{\"type\":\"VM\",\"deleted\":true,\"secret_key\":\"secretkey\",\"history_enabled\":false}");
    }

    @Test
    public void testGetSearchRequest() throws IOException {
        SearchRequest request = _kvRequestBuilder.getSearchRequest(UUID, FROM, SIZE);

        assertNotNull(request);
        assertArrayEquals(new String[] {KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX}, request.indices());

        SearchSourceBuilder sourceBuilder = request.source();
        assertNotNull(sourceBuilder);

        String expectedQuery = IOUtils.resourceToString("search-account-storage-query.json", Charset.defaultCharset(), this.getClass().getClassLoader());
        assertEquals(expectedQuery.trim(), sourceBuilder.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string());
    }

    @Test
    public void testGetUpdateTTLRequest() {
        UpdateRequest request = _kvRequestBuilder.getUpdateTTLRequest(TEMP_STORAGE);

        checkUpdateRequest(request, TEMP_STORAGE, ImmutableMap.of("ttl", TEMP_STORAGE.getTtl(), EntityConstants.EXPIRATION_TIMESTAMP, TEMP_STORAGE.getExpirationTimestamp()));
    }

    @Test
    public void testGetUpdateSecretKeyRequest() {
        UpdateRequest request = _kvRequestBuilder.getUpdateSecretKey(TEMP_STORAGE);

        checkUpdateRequest(request, TEMP_STORAGE, ImmutableMap.of(EntityConstants.SECRET_KEY, TEMP_STORAGE.getSecretKey()));
    }

    @Test
    public void testGetDeletedStoragesRequest() throws IOException {
        SearchRequest request = _kvRequestBuilder.getDeletedStoragesRequest(SIZE, TTL);
        checkSearchRequest(request, TTL, "search-deleted-storages-query.json", null);
    }

    @Test
    public void testGetVmStoragesRequest() throws IOException {
        SearchRequest request = _kvRequestBuilder.getVmStoragesRequest(SIZE, TTL);
        checkSearchRequest(request, TTL, "search-vm-storages-query.json", null);
    }

    @Test
    public void testGetAccountStoragesRequest() throws IOException {
        SearchRequest request = _kvRequestBuilder.getAccountStoragesRequest(SIZE, TTL);
        checkSearchRequest(request, TTL, "search-account-storages-query.json", null);
    }

    @Test
    public void testGetAccountStoragesRequestForAccount() throws IOException {
        SearchRequest request = _kvRequestBuilder.getAccountStoragesRequest(UUID, SIZE, TTL);
        checkSearchRequest(request, TTL, "search-specific-account-storages-query.json", ImmutableMap.of("%UUID%", UUID));
    }

    @Test
    public void testGetScrollRequest() {
        SearchScrollRequest request = _kvRequestBuilder.getScrollRequest(SCROLL_ID, TTL);

        assertNotNull(request);
        assertEquals(SCROLL_ID, request.scrollId());
        assertNotNull(request.scroll());
        assertNotNull(request.scroll().keepAlive());
        assertEquals(TTL, request.scroll().keepAlive().getMillis());
    }

    @Test
    public void testGetExpireTempStorageRequest() throws IOException {
        Request request = _kvRequestBuilder.getExpireTempStorageRequest(TIMESTAMP);
        checkUpdateByQueryRequest(request, "expire-temp-storages-query.json", ImmutableMap.of("%TIMESTAMP%", TIMESTAMP));
    }

    @Test
    public void testGetMarkDeletedRequest() {
        KvStorage storage = new KvStorage(UUID, SECRET_KEY, TTL, TIMESTAMP);

        UpdateRequest request = _kvRequestBuilder.getMarkDeletedRequest(storage);

        checkUpdateRequest(request, storage, ImmutableMap.of(EntityConstants.DELETED, true));
    }

    @Test
    public void getMarkDeletedAccountStorageRequest() throws IOException {
        Request request = _kvRequestBuilder.getMarkDeletedAccountStorageRequest(UUID_LIST);
        checkUpdateByQueryRequest(request, "mark-deleted-account-storages-query.json", ImmutableMap.of("%UUID%", UUID_LIST));
    }

    @Test
    public void getMarkDeletedVmStorageRequest() throws IOException {
        Request request = _kvRequestBuilder.getMarkDeletedVmStorageRequest(UUID_LIST);
        checkUpdateByQueryRequest(request, "mark-deleted-vm-storages-query.json", ImmutableMap.of("%UUID%", UUID_LIST));
    }

    private void testDeleteStorageRequest(KvStorage storage, String source) throws JsonProcessingException {
        DeleteStorageRequest request = _kvRequestBuilder.getDeleteRequest(storage);

        assertNotNull(request);

        checkUpdateRequest(request.getRegistryUpdateRequest(), storage, source);

        assertNotNull(request.getRegistryDeleteRequest());
        assertEquals(storage.getId(), request.getRegistryDeleteRequest().id());

        assertNotNull(request.getStorageIndexRequest());
        assertArrayEquals(new String[] {KvRequestBuilderImpl.STORAGE_INDEX_PREFIX + UUID}, request.getStorageIndexRequest().indices());

        if (storage.getHistoryEnabled()) {
            assertNotNull(request.getHistoryIndexRequest());
            assertArrayEquals(new String[] {KvRequestBuilderImpl.HISTORY_INDEX_PREFIX + UUID}, request.getHistoryIndexRequest().indices());
        }
    }

    private void checkUpdateRequest(IndexRequest request, KvStorage storage, String source) {
        assertNotNull(request);
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX, request.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, request.type());
        assertEquals(DocWriteRequest.OpType.INDEX, request.opType());
        assertEquals(storage.getId(), request.id());
        assertEquals(source, request.source().utf8ToString());
    }

    private void checkUpdateRequest(UpdateRequest request, KvStorage storage, Map<String, Object> parameters) {
        assertNotNull(request);
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX, request.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, request.type());
        assertEquals(storage.getId(), request.id());
        IndexRequest indexRequest = request.doc();
        assertNotNull(indexRequest);
        assertEquals(parameters, indexRequest.sourceAsMap());
    }

    private void checkSearchRequest(SearchRequest request, int ttl, String requestResource, Map<String, Object> params) throws IOException {
        assertNotNull(request);
        assertNotNull(request.scroll());
        assertNotNull(request.scroll().keepAlive());
        assertEquals(ttl, request.scroll().keepAlive().getMillis());

        SearchSourceBuilder sourceBuilder = request.source();
        assertNotNull(sourceBuilder);
        assertEquals(getQuery(requestResource, params), sourceBuilder.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string());
    }

    private void checkUpdateByQueryRequest(Request request, String queryResource, Map<String, Object> scriptParams) throws IOException {
        assertNotNull(request);

        assertEquals("POST", request.getMethod());
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX + "/_update_by_query", request.getEndpoint());
        assertEquals(ImmutableMap.of("conflicts", "proceed"), request.getParameters());

        HttpEntity entity = request.getEntity();
        assertNotNull(entity);
        assertEquals(ContentType.APPLICATION_JSON.toString(), entity.getContentType().getValue());

        assertEquals(getQuery(queryResource, scriptParams), IOUtils.toString(request.getEntity().getContent(), Charset.defaultCharset()));
    }

    private void checkCreateIndexRequest(CreateIndexRequest request, String index) {
        assertNotNull(request);
        assertEquals(index, request.index());
    }

    private static KvStorage get(String id, KvStorage.KvStorageType type, String secretKey, String account, String name, String description, Integer ttl, Long expirationTimestamp,
            boolean historyEnabled, boolean deleted) {
        KvStorage storage = new KvStorage();
        storage.setId(id);
        storage.setType(type);
        storage.setSecretKey(secretKey);
        storage.setAccount(account);
        storage.setName(name);
        storage.setDescription(description);
        storage.setHistoryEnabled(historyEnabled);
        storage.setDeleted(deleted);
        storage.setTtl(ttl);
        storage.setExpirationTimestamp(expirationTimestamp);
        return storage;
    }

    private String getQuery(String queryResource, Map<String, Object> params) throws IOException {
        String expectedQuery = IOUtils.resourceToString(queryResource, Charset.defaultCharset(), this.getClass().getClassLoader());
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, Object> param : params.entrySet()) {
                expectedQuery = expectedQuery.replace(param.getKey(), objectMapper.writeValueAsString(param.getValue()));
            }
        }
        return expectedQuery.trim();
    }
}
