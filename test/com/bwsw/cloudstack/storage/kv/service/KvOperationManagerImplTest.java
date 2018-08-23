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
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import com.bwsw.cloudstack.storage.kv.response.KvValue;
import com.cloud.exception.InvalidParameterValueException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KvOperationManagerImplTest {

    private static final String URL_TEMPLATE = "http://localhost:%d";
    private static final KvStorage STORAGE = new KvStorage("e0123777-921b-4e62-a7cc-8135015ca571", false);
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final Map<String, String> DATA = ImmutableMap.of("key1", "one", "key2", "two");

    private static final String GET_BY_KEY_PATH = "/get/" + STORAGE.getId() + "/" + KEY;
    private static final String GET_BY_KEYS_PATH = "/get/" + STORAGE.getId();

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
        stubFor(get(urlEqualTo(GET_BY_KEY_PATH)).willReturn(aResponse().withHeader("Content-Type", "text/plain").withBody(VALUE)));

        KvOperationResponse response = kvOperationManager.get(STORAGE, KEY);
        assertNotNull(response);
        assert (response instanceof KvValue);
        KvValue result = (KvValue)response;
        assertEquals(VALUE, result.getValue());
    }

    @Test
    public void testGetByKeyNotFoundResponse() {
        expectedException.expect(ServerApiException.class);
        stubFor(get(urlEqualTo(GET_BY_KEY_PATH)).willReturn(aResponse().withStatus(HttpStatus.SC_NOT_FOUND)));

        KvOperationResponse response = kvOperationManager.get(STORAGE, KEY);
        assertNotNull(response);
        assert (response instanceof KvError);
        KvError result = (KvError)response;
        assertEquals(HttpStatus.SC_NOT_FOUND, result.getErrorCode());
    }

    @Test
    public void testGetByKeyInternalErrorResponse() {
        expectedException.expect(ServerApiException.class);
        stubFor(get(urlEqualTo(GET_BY_KEY_PATH)).willReturn(aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)));

        kvOperationManager.get(STORAGE, KEY);
    }

    @Test
    public void testGetByKeyException() {
        expectedException.expect(ServerApiException.class);
        stubFor(get(urlEqualTo(GET_BY_KEY_PATH)).willReturn(aResponse().withFault(Fault.EMPTY_RESPONSE)));

        kvOperationManager.get(STORAGE, KEY);
    }

    @Test
    public void testGetByKeys() throws JsonProcessingException {
        stubFor(post(urlEqualTo(GET_BY_KEYS_PATH)).withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA.keySet())))
                .willReturn(aResponse().withHeader("Content-Type", "application/json").withBody(objectMapper.writeValueAsString(DATA))));

        KvOperationResponse response = kvOperationManager.get(STORAGE, DATA.keySet());
        assertNotNull(response);
        assert (response instanceof KvData);
        KvData result = (KvData)response;
        assertEquals(DATA, result.getItems());
    }

    @Test
    public void testGetByKeysNotFoundResponse() throws JsonProcessingException {
        expectedException.expect(InvalidParameterValueException.class);
        expectedException.expectMessage("KV storage does not exist");

        stubFor(post(urlEqualTo(GET_BY_KEYS_PATH)).withRequestBody(equalToJson(objectMapper.writeValueAsString(DATA.keySet())))
                .willReturn(aResponse().withStatus(HttpStatus.SC_NOT_FOUND)));

        kvOperationManager.get(STORAGE, DATA.keySet());
    }

    @Test
    public void testGetByKeysInternalErrorResponse() {
        expectedException.expect(ServerApiException.class);
        stubFor(post(urlEqualTo(GET_BY_KEYS_PATH)).willReturn(aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)));

        kvOperationManager.get(STORAGE, DATA.keySet());
    }

    @Test
    public void testGetByKeysException() {
        expectedException.expect(ServerApiException.class);
        stubFor(post(urlEqualTo(GET_BY_KEYS_PATH)).willReturn(aResponse().withFault(Fault.EMPTY_RESPONSE)));

        kvOperationManager.get(STORAGE, DATA.keySet());
    }
}
