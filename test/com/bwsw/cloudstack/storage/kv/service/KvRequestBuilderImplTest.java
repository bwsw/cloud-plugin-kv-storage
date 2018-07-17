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
import com.google.common.collect.ImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.action.DocWriteRequest;
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
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(DataProviderRunner.class)
public class KvRequestBuilderImplTest {

    private static final String UUID = "61d12f36-0201-4035-b6fc-c7f768f583f1";
    private static final int FROM = 10;
    private static final int SIZE = 5;
    private static final int TTL = 300000;
    private static final long TIMESTAMP = System.currentTimeMillis();
    private static final String SCROLL_ID = "scrollId";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private KvRequestBuilderImpl _kvRequestBuilder = new KvRequestBuilderImpl();

    @DataProvider
    public static Object[][] storages() {
        return new Object[][] {{get("id val", KvStorage.KvStorageType.ACCOUNT, "account val", "name val", "description val", null, null, true, false),
                "{\"type\":\"ACCOUNT\",\"deleted\":false,\"account\":\"account val\",\"name\":\"name val\",\"description\":\"description val\",\"history_enabled\":true}"},
                {get("id val", KvStorage.KvStorageType.VM, null, null, null, null, null, true, false), "{\"type\":\"VM\",\"deleted\":false,\"history_enabled\":true}"},
                {get("id val", KvStorage.KvStorageType.TEMP, null, null, null, 300000, 1527067849287L, true, false),
                        "{\"type\":\"TEMP\",\"deleted\":false,\"ttl\":300000,\"history_enabled\":true,\"expiration_timestamp\":1527067849287}"}};
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
        IndexRequest request = _kvRequestBuilder.getCreateRequest(storage);

        assertNotNull(request);
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX, request.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, request.type());
        assertEquals(DocWriteRequest.OpType.CREATE, request.opType());
        assertEquals(storage.getId(), request.id());
        assertEquals(source, request.source().utf8ToString());
    }

    @UseDataProvider("storages")
    @Test
    public void testGetUpdateRequest(KvStorage storage, String source) throws JsonProcessingException {
        IndexRequest request = _kvRequestBuilder.getUpdateRequest(storage);

        checkUpdateRequest(request, storage, source);
    }

    @Test
    public void testGetDeleteRequestHistoryEnabledStorage() throws JsonProcessingException {
        KvStorage storage = new KvStorage(UUID, true);

        testDeleteStorageRequest(storage, "{\"type\":\"VM\",\"deleted\":false,\"history_enabled\":true}");
    }

    @Test
    public void testGetDeleteRequestHistoryDisabledStorage() throws JsonProcessingException {
        KvStorage storage = new KvStorage(UUID, false);

        testDeleteStorageRequest(storage, "{\"type\":\"VM\",\"deleted\":false,\"history_enabled\":false}");
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
        KvStorage storage = new KvStorage(UUID, TTL, TIMESTAMP);

        UpdateRequest request = _kvRequestBuilder.getUpdateTTLRequest(storage);

        assertNotNull(request);
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX, request.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, request.type());
        assertEquals(storage.getId(), request.id());
        IndexRequest indexRequest = request.doc();
        assertNotNull(indexRequest);
        Map<String, Object> fields = indexRequest.sourceAsMap();
        assertNotNull(fields);
        assertEquals(2, fields.size());
        assertEquals(storage.getTtl(), fields.get("ttl"));
        assertEquals(storage.getExpirationTimestamp(), fields.get(EntityConstants.EXPIRATION_TIMESTAMP));
    }

    @Test
    public void testGetDeletedStoragesRequest() throws IOException {
        SearchRequest request = _kvRequestBuilder.getDeletedStoragesRequest(SIZE, TTL);

        assertNotNull(request);
        assertNotNull(request.scroll());
        assertNotNull(request.scroll().keepAlive());
        assertEquals(TTL, request.scroll().keepAlive().getMillis());

        SearchSourceBuilder sourceBuilder = request.source();
        assertNotNull(sourceBuilder);

        String expectedQuery = IOUtils.resourceToString("search-deleted-storages-query.json", Charset.defaultCharset(), this.getClass().getClassLoader());
        assertEquals(expectedQuery.trim(), sourceBuilder.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string());
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

        assertNotNull(request);

        assertEquals("POST", request.getMethod());
        assertEquals(KvRequestBuilderImpl.STORAGE_REGISTRY_INDEX + "/_update_by_query", request.getEndpoint());
        assertEquals(ImmutableMap.of("conflicts", "proceed"), request.getParameters());

        HttpEntity entity = request.getEntity();
        assertNotNull(entity);
        assertEquals(ContentType.APPLICATION_JSON.toString(), entity.getContentType().getValue());
        String expectedQuery = IOUtils.resourceToString("expire-temp-storages-query.json", Charset.defaultCharset(), this.getClass().getClassLoader());
        expectedQuery = expectedQuery.replace("%TIMESTAMP%", String.valueOf(TIMESTAMP));
        assertEquals(expectedQuery.trim(), IOUtils.toString(request.getEntity().getContent(), Charset.defaultCharset()));
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

    private static KvStorage get(String id, KvStorage.KvStorageType type, String account, String name, String description, Integer ttl, Long expirationTimestamp,
            boolean historyEnabled, boolean deleted) {
        KvStorage storage = new KvStorage();
        storage.setId(id);
        storage.setType(type);
        storage.setAccount(account);
        storage.setName(name);
        storage.setDescription(description);
        storage.setHistoryEnabled(historyEnabled);
        storage.setDeleted(deleted);
        storage.setTtl(ttl);
        storage.setExpirationTimestamp(expirationTimestamp);
        return storage;
    }
}
