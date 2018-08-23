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
import com.bwsw.cloudstack.storage.kv.response.KvData;
import com.bwsw.cloudstack.storage.kv.response.KvError;
import com.bwsw.cloudstack.storage.kv.response.KvKey;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import com.bwsw.cloudstack.storage.kv.response.KvPair;
import com.bwsw.cloudstack.storage.kv.response.KvResult;
import com.bwsw.cloudstack.storage.kv.response.KvValue;
import com.cloud.exception.InvalidParameterValueException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.Collections;
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

public class KvOperationManagerImplTest {

    private static final String URL_TEMPLATE = "http://localhost:%d";
    private static final KvStorage STORAGE = new KvStorage("e0123777-921b-4e62-a7cc-8135015ca571", false);
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final Map<String, String> DATA = ImmutableMap.of("key1", "one", "key2", "two");

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(options().dynamicPort(), true);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ObjectMapper objectMapper = new ObjectMapper();

    private KvOperationManagerImpl kvOperationManager;

    @Before
    public void before() {
        kvOperationManager = new KvOperationManagerImpl(String.format(URL_TEMPLATE, wireMockRule.port()));
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
        expectedException.expect(ServerApiException.class);
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
        stubFor(getGetByKeysPath().willReturn(aResponse().withHeader("Content-Type", "application/json").withBody(objectMapper.writeValueAsString(DATA))));

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
        stubFor(getSetValuesPath().willReturn(aResponse().withHeader("Content-Type", "application/json").withBody(objectMapper.writeValueAsString(result))));

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
    public void testDeleteNotFoundResponse() {
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

    private MappingBuilder getGetByKeyPath() {
        return get(urlEqualTo("/get/" + STORAGE.getId() + "/" + KEY));
    }

    private MappingBuilder getGetByKeysPath() {
        try {
            return post(urlEqualTo("/get/" + STORAGE.getId())).withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA.keySet())));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MappingBuilder getSetValuePath() {
        return put(urlEqualTo("/set/" + STORAGE.getId() + "/" + KEY)).withRequestBody(equalTo(VALUE));
    }

    private MappingBuilder getSetValuesPath() {
        try {
            return put(urlEqualTo("/set/" + STORAGE.getId())).withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private MappingBuilder getDeleteKeyPath() {
        return delete(urlEqualTo("/delete/" + STORAGE.getId() + "/" + KEY));
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

    private <T extends KvOperationResponse> void testNotFoundResponse(Supplier<MappingBuilder> requestBuilder, Supplier<T> responseSupplier) {
        expectNonexistentStorage();
        stubFor(requestBuilder.get().willReturn(aResponse().withStatus(HttpStatus.SC_NOT_FOUND)));

        responseSupplier.get();
    }

    private <T extends KvOperationResponse> void testInternalErrorResponse(Supplier<MappingBuilder> requestBuilder, Supplier<T> responseSupplier) {
        expectedException.expect(ServerApiException.class);
        stubFor(requestBuilder.get().willReturn(aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)));

        responseSupplier.get();
    }

    private <T extends KvOperationResponse> void testException(Supplier<MappingBuilder> requestBuilder, Supplier<T> responseSupplier) {
        expectedException.expect(ServerApiException.class);
        stubFor(requestBuilder.get().willReturn(aResponse().withFault(Fault.EMPTY_RESPONSE)));

        responseSupplier.get();
    }

    private void expectNonexistentStorage() {
        expectedException.expect(InvalidParameterValueException.class);
        expectedException.expectMessage("KV storage does not exist");
    }
}
