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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;

public class KvOperationManagerImpl implements KvOperationManager {

    private static final int TIMEOUT = 3000;
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final ContentType JSON_CONTENT_TYPE = ContentType.create("application/json");

    private static final Logger s_logger = Logger.getLogger(KvStorageManagerImpl.class);

    @FunctionalInterface
    private interface CheckedBiFunction<T, U, R, E extends Exception> {
        R apply(T t, U u) throws E;
    }

    @FunctionalInterface
    private interface CheckedSupplier<T, E extends Exception> {
        T get() throws E;
    }

    private final CloseableHttpClient _httpClient;
    private final String _url;
    private final ObjectMapper objectMapper;

    public KvOperationManagerImpl(String url) {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(TIMEOUT).setConnectionRequestTimeout(TIMEOUT).setSocketTimeout(TIMEOUT).build();
        _httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
        this._url = StringUtils.appendIfMissing(url, "/");
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public KvOperationResponse get(KvStorage storage, String key) {
        return execute(() -> new HttpGet(String.format("%sget/%s/%s", _url, encode(storage.getId()), encode(key))), (statusCode, entity) -> {
            if (statusCode == HttpStatus.SC_OK) {
                return new KvValue(EntityUtils.toString(entity, CHARSET));
            } else {
                return new KvError(statusCode);
            }
        });
    }

    @Override
    public KvOperationResponse get(KvStorage storage, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return new KvData();
        }
        return execute(() -> {
            StringEntity entity = new StringEntity(objectMapper.writeValueAsString(keys), JSON_CONTENT_TYPE);
            HttpPost request = new HttpPost(String.format("%sget/%s", _url, encode(storage.getId())));
            request.setEntity(entity);
            return request;
        }, (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                @SuppressWarnings("unchecked") Map<String, String> items = objectMapper.readValue(EntityUtils.toString(entity, CHARSET), Map.class);
                return new KvData(items);
            case HttpStatus.SC_NOT_FOUND:
                throw new InvalidParameterValueException("KV storage does not exist");
            default:
                throw new RuntimeException("Unexpected KV get by keys operation status: " + statusCode);
            }
        });
    }

    private String encode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // never happens as UTF-8 is always supported
            throw new UnsupportedOperationException();
        }
    }

    private KvOperationResponse execute(CheckedSupplier<HttpUriRequest, Exception> requestSupplier,
            CheckedBiFunction<Integer, HttpEntity, KvOperationResponse, Exception> responseFactory) {
        CloseableHttpResponse response = null;
        try {
            response = _httpClient.execute(requestSupplier.get());
            return responseFactory.apply(response.getStatusLine().getStatusCode(), response.getEntity());
        } catch (InvalidParameterException e) {
            throw e;
        } catch (Exception e) {
            s_logger.error("Unable to execute storage operation", e);
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to execute KV storage operation");
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // do nothing
                    s_logger.error("Unable to close response", e);
                }
            }
        }
    }
}
