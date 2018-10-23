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
import com.bwsw.cloudstack.storage.kv.response.KvHistoryResult;
import com.bwsw.cloudstack.storage.kv.response.KvKey;
import com.bwsw.cloudstack.storage.kv.response.KvKeys;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import com.bwsw.cloudstack.storage.kv.response.KvPair;
import com.bwsw.cloudstack.storage.kv.response.KvResult;
import com.bwsw.cloudstack.storage.kv.response.KvSuccess;
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
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KvOperationManagerImpl implements KvOperationManager {

    private static final int TIMEOUT = 3000;
    private static final String SECRET_KEY_HEADER = "Secret-Key";
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private static final ContentType JSON_CONTENT_TYPE = ContentType.create("application/json");
    private static final ContentType TEXT_PLAIN_CONTENT_TYPE = ContentType.create("text/plain");

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
    private final ExceptionFactory exceptionFactory;

    public KvOperationManagerImpl(String url, ExceptionFactory exceptionFactory) {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(TIMEOUT).setConnectionRequestTimeout(TIMEOUT).setSocketTimeout(TIMEOUT).build();
        _httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
        this._url = StringUtils.appendIfMissing(url, "/");
        this.objectMapper = new ObjectMapper();
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public KvOperationResponse get(KvStorage storage, String key) {
        return execute(() -> new HttpGet(String.format("%sget/%s/%s", _url, encode(storage.getId()), encode(key))), storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                return new KvValue(EntityUtils.toString(entity, CHARSET));
            case HttpStatus.SC_NOT_FOUND:
                return new KvError(statusCode);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvOperationResponse get(KvStorage storage, Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return new KvData();
        }
        return execute(() -> {
            StringEntity entity = new StringEntity(objectMapper.writeValueAsString(keys), JSON_CONTENT_TYPE);
            HttpPost request = new HttpPost(String.format("%sget/%s", _url, encode(storage.getId())));
            request.setEntity(entity);
            return request;
        }, storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                @SuppressWarnings("unchecked") Map<String, String> items = objectMapper.readValue(EntityUtils.toString(entity, CHARSET), Map.class);
                return new KvData(items);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvPair set(KvStorage storage, String key, String value) {
        if (key == null || key.isEmpty()) {
            throw new InvalidParameterValueException("Null or empty key");
        }
        return execute(() -> {
            StringEntity entity = new StringEntity(value, TEXT_PLAIN_CONTENT_TYPE);
            HttpPut request = new HttpPut(String.format("%sset/%s/%s", _url, encode(storage.getId()), encode(key)));
            request.setEntity(entity);
            return request;
        }, storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                return new KvPair(key, value);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            case HttpStatus.SC_BAD_REQUEST:
                throw new InvalidParameterValueException("Key/value pair is invalid");
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvResult set(KvStorage storage, Map<String, String> data) {
        if (data == null || data.isEmpty()) {
            return new KvResult();
        }
        return execute(() -> {
            StringEntity entity = new StringEntity(objectMapper.writeValueAsString(data), JSON_CONTENT_TYPE);
            HttpPut request = new HttpPut(String.format("%sset/%s", _url, encode(storage.getId())));
            request.setEntity(entity);
            return request;
        }, storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                @SuppressWarnings("unchecked") Map<String, Boolean> items = objectMapper.readValue(EntityUtils.toString(entity, CHARSET), Map.class);
                return new KvResult(items);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvKey delete(KvStorage storage, String key) {
        return execute(() -> new HttpDelete(String.format("%sdelete/%s/%s", _url, storage.getId(), encode(key))), storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                return new KvKey(key);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvResult delete(KvStorage storage, Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return new KvResult();
        }
        return execute(() -> {
            StringEntity entity = new StringEntity(objectMapper.writeValueAsString(keys), JSON_CONTENT_TYPE);
            HttpPost request = new HttpPost(String.format("%sdelete/%s", _url, encode(storage.getId())));
            request.setEntity(entity);
            return request;
        }, storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                @SuppressWarnings("unchecked") Map<String, Boolean> items = objectMapper.readValue(EntityUtils.toString(entity, CHARSET), Map.class);
                return new KvResult(items);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvKeys list(KvStorage storage) {
        return execute(() -> new HttpGet(String.format("%slist/%s", _url, encode(storage.getId()))), storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                @SuppressWarnings("unchecked") List<String> items = objectMapper.readValue(EntityUtils.toString(entity, CHARSET), List.class);
                return new KvKeys(items);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvOperationResponse clear(KvStorage storage) {
        return execute(() -> new HttpPost(String.format("%sclear/%s", _url, encode(storage.getId()))), storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                return new KvSuccess();
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            case HttpStatus.SC_CONFLICT:
                return new KvError(statusCode);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvHistoryResult getHistory(KvStorage storage, List<String> keys, List<String> operations, Long start, Long end, List<String> sort, Integer page, Integer size,
            Long scroll) {
        if (storage.getHistoryEnabled() == null || !storage.getHistoryEnabled()) {
            throw exceptionFactory.getException(InvalidParameterValueCode.HISTORY_DISABLED_STORAGE);
        }
        return execute(() -> {
            Map<String, String> params = new HashMap<>();
            put(params, "keys", keys);
            put(params, "operations", operations);
            if (start != null) {
                params.put("start", start.toString());
            }
            if (end != null) {
                params.put("end", end.toString());
            }
            put(params, "sort", sort);
            if (page != null) {
                params.put("page", page.toString());
            }
            if (size != null) {
                params.put("size", size.toString());
            }
            if (scroll != null) {
                params.put("scroll", scroll.toString());
            }
            String query = params.entrySet().stream().map(e -> e.getKey() + "=" + encode(e.getValue())).collect(Collectors.joining("&"));
            return new HttpGet(String.format("%shistory/%s?%s", _url, encode(storage.getId()), query));
        }, storage.getSecretKey(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                return objectMapper.readValue(EntityUtils.toString(entity), KvHistoryResult.class);
            case HttpStatus.SC_BAD_REQUEST:
                throw exceptionFactory.getException(InvalidParameterValueCode.INVALID_HISTORY_REQUEST);
            case HttpStatus.SC_NOT_FOUND:
                throw exceptionFactory.getException(InvalidParameterValueCode.NONEXISTENT_STORAGE);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    @Override
    public KvHistoryResult getHistory(String scrollId, long timeout) {
        return execute(() -> {
            Map<String, Object> body = new HashMap<>();
            body.put("scrollId", scrollId);
            body.put("timeout", timeout);
            StringEntity entity = new StringEntity(objectMapper.writeValueAsString(body), JSON_CONTENT_TYPE);
            HttpPost request = new HttpPost(String.format("%shistory", _url));
            request.setEntity(entity);
            return request;
        }, Optional.empty(), (statusCode, entity) -> {
            switch (statusCode) {
            case HttpStatus.SC_OK:
                return objectMapper.readValue(EntityUtils.toString(entity), KvHistoryResult.class);
            case HttpStatus.SC_BAD_REQUEST:
                throw exceptionFactory.getException(InvalidParameterValueCode.INVALID_SCROLL_ID);
            default:
                throw exceptionFactory.getKvOperationException(statusCode);
            }
        });
    }

    private void put(Map<String, String> map, String key, List<String> values) {
        if (values != null && !values.isEmpty()) {
            map.put(key, values.stream().collect(Collectors.joining(",")));
        }
    }

    private String encode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // never happens as UTF-8 is always supported
            throw new UnsupportedOperationException();
        }
    }

    private <T extends KvOperationResponse> T execute(CheckedSupplier<HttpUriRequest, Exception> requestSupplier, String secretKey,
            CheckedBiFunction<Integer, HttpEntity, T, Exception> responseFactory) {
        return execute(requestSupplier, Optional.ofNullable(secretKey), responseFactory);
    }

    private <T extends KvOperationResponse> T execute(CheckedSupplier<HttpUriRequest, Exception> requestSupplier, Optional<String> secretKey,
            CheckedBiFunction<Integer, HttpEntity, T, Exception> responseFactory) {
        CloseableHttpResponse response = null;
        try {
            HttpUriRequest request = requestSupplier.get();
            secretKey.ifPresent(s -> request.setHeader(SECRET_KEY_HEADER, s));
            response = _httpClient.execute(request);
            return responseFactory.apply(response.getStatusLine().getStatusCode(), response.getEntity());
        } catch (InvalidParameterValueException e) {
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
