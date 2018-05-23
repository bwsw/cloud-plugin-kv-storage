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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(DataProviderRunner.class)
public class KvRequestBuilderImplTest {

    @DataProvider
    public static Object[][] storages() {
        return new Object[][] {
                { get("id val", "account val", "name val", "description val", null, null),
                        "{\"account\":\"account val\",\"name\":\"name val\",\"description\":\"description val\"}"},
                { get("id val", null, null, null, null, null), "{}"},
                { get("id val", null, null, null, 300000, 1527067849287L), "{\"ttl\":300000,\"expiration_timestamp\":1527067849287}"}
        };
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private KvRequestBuilderImpl _kvRequestBuilder = new KvRequestBuilderImpl();

    @UseDataProvider("storages")
    @Test
    public void testGetCreateRequest(KvStorage storage, String source) throws JsonProcessingException {
        IndexRequest request = _kvRequestBuilder.getCreateRequest(storage);

        assertNotNull(request);
        assertEquals(KvRequestBuilderImpl.STORAGE_INDEX, request.index());
        assertEquals(KvRequestBuilderImpl.STORAGE_TYPE, request.type());
        assertEquals(DocWriteRequest.OpType.CREATE, request.opType());
        assertEquals(storage.getId(), request.id());
        assertEquals(source, request.source().utf8ToString());
    }

    private static KvStorage get(String id, String account, String name, String description, Integer ttl, Long expirationTimestamp) {
        KvStorage storage = new KvStorage();
        storage.setId(id);
        storage.setAccount(account);
        storage.setName(name);
        storage.setDescription(description);
        storage.setTtl(ttl);
        storage.setExpirationTimestamp(expirationTimestamp);
        return storage;
    }
}
