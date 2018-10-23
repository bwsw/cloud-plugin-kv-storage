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

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.exception.ExceptionFactory;
import com.bwsw.cloudstack.storage.kv.exception.InvalidParameterValueCode;
import com.bwsw.cloudstack.storage.kv.response.KvData;
import com.bwsw.cloudstack.storage.kv.response.KvError;
import com.bwsw.cloudstack.storage.kv.response.KvHistory;
import com.bwsw.cloudstack.storage.kv.response.KvHistoryResult;
import com.bwsw.cloudstack.storage.kv.response.KvKey;
import com.bwsw.cloudstack.storage.kv.response.KvKeys;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import com.bwsw.cloudstack.storage.kv.response.KvPair;
import com.bwsw.cloudstack.storage.kv.response.KvResult;
import com.bwsw.cloudstack.storage.kv.response.KvSuccess;
import com.bwsw.cloudstack.storage.kv.response.KvValue;
import com.cloud.exception.InvalidParameterValueException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KvOperationManagerImplTest {

    private static final String URL_TEMPLATE = "http://localhost:%d";
    private static final String SECRET_KEY_HEADER = "Secret-Key";
    private static final KvStorage STORAGE = new KvStorage("e0123777-921b-4e62-a7cc-8135015ca571", "secret", false);
    private static final KvStorage HISTORY_ENABLED_STORAGE = new KvStorage("c0123777-921b-4e62-a7cc-8135015ca571", "secret", true);
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final Map<String, String> DATA = ImmutableMap.of("key1", "one", "key2", "two");
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String SCROLL_ID = "scroll id";
    private static final long TIMEOUT = 120000;
    private static final Map<String, Object> SCROLL_REQUEST = ImmutableMap.of("scrollId", SCROLL_ID, "timeout", TIMEOUT);
    private static final KvHistoryResult KV_HISTORY_RESULT;

    static {
        KV_HISTORY_RESULT = new KvHistoryResult();

        KvHistory history = new KvHistory();
        history.setKey("first");
        history.setOperation("set");
        history.setValue("one");
        history.setTimestamp(1539748473600L);
        List<KvHistory> items = ImmutableList.of(history);

        KV_HISTORY_RESULT.setTotal((long)items.size());
        KV_HISTORY_RESULT.setPage(1);
        KV_HISTORY_RESULT.setSize(items.size());
        KV_HISTORY_RESULT.setItems(items);
    }

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort(), true);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private ExceptionFactory _exceptionFactory;

    private ObjectMapper objectMapper = new ObjectMapper()
            .disable(MapperFeature.AUTO_DETECT_CREATORS, MapperFeature.AUTO_DETECT_FIELDS, MapperFeature.AUTO_DETECT_GETTERS, MapperFeature.AUTO_DETECT_IS_GETTERS);

    private KvOperationManagerImpl kvOperationManager;

    @Before
    public void before() {
        kvOperationManager = new KvOperationManagerImpl(String.format(URL_TEMPLATE, wireMockRule.port()), _exceptionFactory);
    }

    @Test
    public void testGetByKey() {
        stubFor(getGetByKeyPath().willReturn(aResponse().withHeader("Content-Type", "text/plain").withBody(VALUE)));

        KvOperationResponse response = kvOperationManager.get(STORAGE, KEY);
        assertNotNull(response);
        assert (response instanceof KvValue);
        KvValue result = (KvValue)response;
        assertEquals(VALUE, result.getValue());
    }

    @Test
    public void testGetByKeyNotFoundResponse() {
        stubFor(getGetByKeyPath().willReturn(aResponse().withStatus(HttpStatus.SC_NOT_FOUND)));

        KvOperationResponse response = kvOperationManager.get(STORAGE, KEY);
        assertNotNull(response);
        assert (response instanceof KvError);
        KvError result = (KvError)response;
        assertEquals(HttpStatus.SC_NOT_FOUND, result.getErrorCode());
    }

    @Test
    public void testGetByKeyInternalErrorResponse() {
        testInternalErrorResponse(this::getGetByKeyPath, getByKeySupplier());
    }

    @Test
    public void testGetByKeyException() {
        testException(this::getGetByKeyPath, getByKeySupplier());
    }

    @Test
    public void testGetByKeys() throws JsonProcessingException {
        stubFor(getGetByKeysPath().willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(DATA))));

        KvOperationResponse response = kvOperationManager.get(STORAGE, DATA.keySet());
        assertNotNull(response);
        assert (response instanceof KvData);
        KvData result = (KvData)response;
        assertEquals(DATA, result.getItems());
    }

    @Test
    public void testGetByKeysNullKeyCollection() {
        KvOperationResponse response = kvOperationManager.get(STORAGE, (Collection<String>)null);

        assertNotNull(response);
        assertTrue(response instanceof KvData);
        assertEquals(Collections.emptyMap(), ((KvData)response).getItems());
    }

    @Test
    public void testGetByKeysEmptyKeyCollection() {
        KvOperationResponse response = kvOperationManager.get(STORAGE, Collections.emptySet());

        assertNotNull(response);
        assertTrue(response instanceof KvData);
        assertEquals(Collections.emptyMap(), ((KvData)response).getItems());
    }

    @Test
    public void testGetByKeysNotFoundResponse() {
        testNotFoundResponse(this::getGetByKeysPath, getByKeysSupplier());
    }

    @Test
    public void testGetByKeysInternalErrorResponse() {
        testInternalErrorResponse(this::getGetByKeysPath, getByKeysSupplier());
    }

    @Test
    public void testGetByKeysException() {
        testException(this::getGetByKeysPath, getByKeysSupplier());
    }

    @Test
    public void testSetValue() {
        stubFor(getSetValuePath().willReturn(aResponse().withStatus(HttpStatus.SC_OK)));

        KvPair response = kvOperationManager.set(STORAGE, KEY, VALUE);
        assertNotNull(response);
        assertEquals(KEY, response.getKey());
        assertEquals(VALUE, response.getValue());
    }

    @Test
    public void testSetNullKey() {
        expectedException.expect(InvalidParameterValueException.class);
        kvOperationManager.set(STORAGE, null, VALUE);
    }

    @Test
    public void testSetEmptyKey() {
        expectedException.expect(InvalidParameterValueException.class);
        kvOperationManager.set(STORAGE, "", VALUE);
    }

    @Test
    public void testSetValueNotFoundResponse() {
        testNotFoundResponse(this::getSetValuePath, setValueSupplier());
    }

    @Test
    public void testSetValueBadRequestResponse() {
        expectedException.expect(InvalidParameterValueException.class);

        stubFor(getSetValuePath().willReturn(aResponse().withStatus(HttpStatus.SC_BAD_REQUEST)));

        kvOperationManager.set(STORAGE, KEY, VALUE);
    }

    @Test
    public void testSetValueInternalErrorResponse() {
        testInternalErrorResponse(this::getSetValuePath, setValueSupplier());
    }

    @Test
    public void testSetValueException() {
        testException(this::getSetValuePath, setValueSupplier());
    }

    @Test
    public void testSetValues() throws JsonProcessingException {
        Map<String, Boolean> result = DATA.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> true));
        stubFor(getSetValuesPath().willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(result))));

        KvResult response = kvOperationManager.set(STORAGE, DATA);
        assertNotNull(response);
        assertEquals(result, response.getItems());
    }

    @Test
    public void testSetNullData() {
        KvResult response = kvOperationManager.set(STORAGE, null);
        assertNotNull(response);
        assertEquals(Collections.emptyMap(), response.getItems());
    }

    @Test
    public void testSetEmptyData() {
        KvResult response = kvOperationManager.set(STORAGE, Collections.emptyMap());
        assertNotNull(response);
        assertEquals(Collections.emptyMap(), response.getItems());
    }

    @Test
    public void testSetValuesNotFoundResponse() {
        testNotFoundResponse(this::getSetValuesPath, setValuesSupplier());
    }

    @Test
    public void testSetValuesInternalErrorResponse() {
        testInternalErrorResponse(this::getSetValuesPath, setValuesSupplier());
    }

    @Test
    public void testSetValuesException() {
        testException(this::getSetValuesPath, setValuesSupplier());
    }

    @Test
    public void testDeleteKey() {
        stubFor(getDeleteKeyPath().willReturn(aResponse().withStatus(HttpStatus.SC_OK)));

        KvKey response = kvOperationManager.delete(STORAGE, KEY);
        assertNotNull(response);
        assertEquals(KEY, response.getKey());
    }

    @Test
    public void testDeleteKeyNotFoundResponse() {
        testNotFoundResponse(this::getDeleteKeyPath, deleteKeySupplier());
    }

    @Test
    public void testDeleteKeyInternalErrorResponse() {
        testInternalErrorResponse(this::getDeleteKeyPath, deleteKeySupplier());
    }

    @Test
    public void testDeleteKeyException() {
        testException(this::getDeleteKeyPath, deleteKeySupplier());
    }

    @Test
    public void testDeleteKeys() throws JsonProcessingException {
        Map<String, Boolean> result = DATA.keySet().stream().collect(Collectors.toMap(Function.identity(), k -> true));
        stubFor(getDeleteKeysPath().willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(result))));

        KvResult response = kvOperationManager.delete(STORAGE, DATA.keySet());
        assertNotNull(response);
        assertEquals(result, response.getItems());
    }

    @Test
    public void testDeleteKeysNullKeyCollection() {
        KvResult response = kvOperationManager.delete(STORAGE, (Collection<String>)null);

        assertNotNull(response);
        assertEquals(Collections.emptyMap(), response.getItems());
    }

    @Test
    public void testDeleteKeysEmptyKeyCollection() {
        KvResult response = kvOperationManager.delete(STORAGE, Collections.emptySet());

        assertNotNull(response);
        assertEquals(Collections.emptyMap(), response.getItems());
    }

    @Test
    public void testDeleteKeysNotFoundResponse() {
        testNotFoundResponse(this::getDeleteKeysPath, deleteKeysSupplier());
    }

    @Test
    public void testDeleteKeysInternalErrorResponse() {
        testInternalErrorResponse(this::getDeleteKeysPath, deleteKeysSupplier());
    }

    @Test
    public void testDeleteKeysException() {
        testException(this::getDeleteKeysPath, deleteKeysSupplier());
    }

    @Test
    public void testList() throws JsonProcessingException {
        stubFor(getListPath().willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(DATA.keySet()))));

        KvKeys response = kvOperationManager.list(STORAGE);
        assertNotNull(response);
        assertEquals(new ArrayList<>(DATA.keySet()), response.getItems());
    }

    @Test
    public void testListNotFoundResponse() {
        testNotFoundResponse(this::getListPath, listSupplier());
    }

    @Test
    public void testListInternalErrorResponse() {
        testInternalErrorResponse(this::getListPath, listSupplier());
    }

    @Test
    public void testListException() {
        testException(this::getListPath, listSupplier());
    }

    @Test
    public void testClear() {
        stubFor(getClearPath().willReturn(aResponse().withStatus(HttpStatus.SC_OK)));

        KvOperationResponse response = kvOperationManager.clear(STORAGE);
        assertNotNull(response);
        assertTrue(response instanceof KvSuccess);
        assertTrue(((KvSuccess)response).isSuccess());
    }

    @Test
    public void testClearConflictResponse() {
        stubFor(getClearPath().willReturn(aResponse().withStatus(HttpStatus.SC_CONFLICT)));

        KvOperationResponse response = kvOperationManager.clear(STORAGE);
        assertNotNull(response);
        assertTrue(response instanceof KvError);
        assertEquals(HttpStatus.SC_CONFLICT, ((KvError)response).getErrorCode());
    }

    @Test
    public void testClearNotFoundResponse() {
        testNotFoundResponse(this::getClearPath, clearSupplier());
    }

    @Test
    public void testClearInternalErrorResponse() {
        testInternalErrorResponse(this::getClearPath, clearSupplier());
    }

    @Test
    public void testClearException() {
        testException(this::getClearPath, clearSupplier());
    }

    @Test
    public void testGetHistoryAllParameters() throws JsonProcessingException {
        List<String> keys = ImmutableList.of("key1", "key2");
        List<String> operations = ImmutableList.of("set", "delete");
        long start = 1539748470000L;
        long end = start + 86400;
        List<String> sort = ImmutableList.of("timestamp", "key");
        int page = 2;
        int size = 5;
        long scroll = 60000;

        stubFor(get(urlEqualTo("/history/" + HISTORY_ENABLED_STORAGE.getId()
                + "?operations=set%2Cdelete&size=5&keys=key1%2Ckey2&start=1539748470000&scroll=60000&end=1539748556400&sort=timestamp%2Ckey&page=2"))
                .willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(KV_HISTORY_RESULT))));

        KvHistoryResult result = kvOperationManager.getHistory(HISTORY_ENABLED_STORAGE, keys, operations, start, end, sort, page, size, scroll);
        assertEquals(KV_HISTORY_RESULT, result);
    }

    @Test
    public void testGetHistory() throws JsonProcessingException {
        stubFor(getHistoryPath().willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(KV_HISTORY_RESULT))));

        KvHistoryResult result = kvOperationManager.getHistory(HISTORY_ENABLED_STORAGE, null, null, null, null, null, null, null, null);
        assertEquals(KV_HISTORY_RESULT, result);
    }

    @Test
    public void testGetHistoryNotFoundResponse() {
        testNotFoundResponse(this::getHistoryPath, historySupplier());
    }

    @Test
    public void testGetHistoryInternalError() {
        testInternalErrorResponse(this::getHistoryPath, historySupplier());
    }

    @Test
    public void testGetHistoryException() {
        testException(this::getHistoryPath, historySupplier());
    }

    @Test
    public void testGetHistoryHistoryDisabledStorage() {
        InvalidParameterValueException exception = new InvalidParameterValueException("history is not supported");
        expectedException.expect(exception.getClass());
        when(_exceptionFactory.getException(InvalidParameterValueCode.HISTORY_DISABLED_STORAGE)).thenReturn(exception);

        kvOperationManager.getHistory(STORAGE, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testGetHistoryBadRequest() {
        InvalidParameterValueException exception = new InvalidParameterValueException("Invalid KV history request");
        expectedException.expect(exception.getClass());
        when(_exceptionFactory.getException(InvalidParameterValueCode.INVALID_HISTORY_REQUEST)).thenReturn(exception);

        stubFor(getHistoryPath().willReturn(aResponse().withStatus(HttpStatus.SC_BAD_REQUEST)));

        kvOperationManager.getHistory(HISTORY_ENABLED_STORAGE, null, null, null, null, null, null, null, null);
    }

    @Test
    public void testGetHistoryScroll() throws JsonProcessingException {
        stubFor(getHistoryScrollPath().willReturn(aResponse().withHeader(CONTENT_TYPE_HEADER, JSON_CONTENT_TYPE).withBody(objectMapper.writeValueAsString(KV_HISTORY_RESULT))));

        KvHistoryResult result = kvOperationManager.getHistory(SCROLL_ID, TIMEOUT);
        assertEquals(KV_HISTORY_RESULT, result);
    }

    @Test
    public void testGetHistoryScrollBadRequest() {
        InvalidParameterValueException exception = new InvalidParameterValueException("invalid scroll");
        expectedException.expect(exception.getClass());
        when(_exceptionFactory.getException(InvalidParameterValueCode.INVALID_SCROLL_ID)).thenReturn(exception);

        stubFor(getHistoryScrollPath().willReturn(aResponse().withStatus(HttpStatus.SC_BAD_REQUEST)));

        kvOperationManager.getHistory(SCROLL_ID, TIMEOUT);
    }

    @Test
    public void testGetHistoryScrollInternalError() {
        testInternalErrorResponse(this::getHistoryScrollPath, historyScrollSupplier());
    }

    @Test
    public void testGetHistoryScrollException() {
        testException(this::getHistoryScrollPath, historyScrollSupplier());
    }

    private MappingBuilder getGetByKeyPath() {
        return get(urlEqualTo("/get/" + STORAGE.getId() + "/" + KEY)).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()));
    }

    private MappingBuilder getGetByKeysPath() {
        try {
            return post(urlEqualTo("/get/" + STORAGE.getId())).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()))
                    .withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA.keySet())));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MappingBuilder getSetValuePath() {
        return put(urlEqualTo("/set/" + STORAGE.getId() + "/" + KEY)).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey())).withRequestBody(equalTo(VALUE));
    }

    private MappingBuilder getSetValuesPath() {
        try {
            return put(urlEqualTo("/set/" + STORAGE.getId())).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()))
                    .withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MappingBuilder getDeleteKeyPath() {
        return delete(urlEqualTo("/delete/" + STORAGE.getId() + "/" + KEY)).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()));
    }

    private MappingBuilder getDeleteKeysPath() {
        try {
            return post(urlEqualTo("/delete/" + STORAGE.getId())).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()))
                    .withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA.keySet())));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MappingBuilder getListPath() {
        return get(urlEqualTo("/list/" + STORAGE.getId())).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()));
    }

    private MappingBuilder getClearPath() {
        return post(urlEqualTo("/clear/" + STORAGE.getId())).withHeader(SECRET_KEY_HEADER, equalTo(STORAGE.getSecretKey()));
    }

    private MappingBuilder getHistoryPath() {
        return get(urlEqualTo("/history/" + HISTORY_ENABLED_STORAGE.getId())).withHeader(SECRET_KEY_HEADER, equalTo(HISTORY_ENABLED_STORAGE.getSecretKey()));
    }

    private MappingBuilder getHistoryScrollPath() {
        try {
            return post(urlEqualTo("/history")).withRequestBody(equalToJson(objectMapper.writeValueAsString(SCROLL_REQUEST)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Supplier<KvOperationResponse> getByKeySupplier() {
        return () -> kvOperationManager.get(STORAGE, KEY);
    }

    private Supplier<KvOperationResponse> getByKeysSupplier() {
        return () -> kvOperationManager.get(STORAGE, DATA.keySet());
    }

    private Supplier<KvPair> setValueSupplier() {
        return () -> kvOperationManager.set(STORAGE, KEY, VALUE);
    }

    private Supplier<KvResult> setValuesSupplier() {
        return () -> kvOperationManager.set(STORAGE, DATA);
    }

    private Supplier<KvKey> deleteKeySupplier() {
        return () -> kvOperationManager.delete(STORAGE, KEY);
    }

    private Supplier<KvResult> deleteKeysSupplier() {
        return () -> kvOperationManager.delete(STORAGE, DATA.keySet());
    }

    private Supplier<KvKeys> listSupplier() {
        return () -> kvOperationManager.list(STORAGE);
    }

    private Supplier<KvOperationResponse> clearSupplier() {
        return () -> kvOperationManager.clear(STORAGE);
    }

    private Supplier<KvHistoryResult> historySupplier() {
        return () -> kvOperationManager.getHistory(HISTORY_ENABLED_STORAGE, null, null, null, null, null, null, null, null);
    }

    private Supplier<KvHistoryResult> historyScrollSupplier() {
        return () -> kvOperationManager.getHistory(SCROLL_ID, TIMEOUT);
    }

    private <T extends KvOperationResponse> void testNotFoundResponse(Supplier<MappingBuilder> requestBuilder, Supplier<T> responseSupplier) {
        expectedException.expect(InvalidParameterValueException.class);
        when(_exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE)).thenReturn(new InvalidParameterValueException("not found"));
        stubFor(requestBuilder.get().willReturn(aResponse().withStatus(HttpStatus.SC_NOT_FOUND)));

        responseSupplier.get();
    }

    private <T extends KvOperationResponse> void testInternalErrorResponse(Supplier<MappingBuilder> requestBuilder, Supplier<T> responseSupplier) {
        expectedException.expect(ServerApiException.class);
        when(_exceptionFactory.getKvOperationException(anyInt())).thenReturn(new RuntimeException());
        stubFor(requestBuilder.get().willReturn(aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)));

        responseSupplier.get();
    }

    private <T extends KvOperationResponse> void testException(Supplier<MappingBuilder> requestBuilder, Supplier<T> responseSupplier) {
        expectedException.expect(ServerApiException.class);
        stubFor(requestBuilder.get().willReturn(aResponse().withFault(Fault.EMPTY_RESPONSE)));

        responseSupplier.get();
    }
}
