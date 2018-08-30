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

package com.bwsw.cloudstack.storage.kv.client;

import com.bwsw.cloudstack.storage.kv.util.HttpUtils;
import com.cloud.utils.component.ComponentLifecycleBase;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;

import javax.naming.ConfigurationException;
import java.util.Map;

import static com.bwsw.cloudstack.storage.kv.service.KvStorageManager.KvStorageElasticsearchList;
import static com.bwsw.cloudstack.storage.kv.service.KvStorageManager.KvStorageElasticsearchPassword;
import static com.bwsw.cloudstack.storage.kv.service.KvStorageManager.KvStorageElasticsearchUsername;

public class KvStorageClientManagerImpl extends ComponentLifecycleBase implements KvStorageClientManager {

    private static final Logger s_logger = Logger.getLogger(KvStorageClientManagerImpl.class);

    private RestHighLevelClient _restHighLevelClient;

    @Override
    public boolean configure(String name, Map<String, Object> params) throws ConfigurationException {
        try {
            RestClientBuilder restClientBuilder = RestClient.builder(HttpUtils.getHttpHosts(KvStorageElasticsearchList.value()).toArray(new HttpHost[] {}));
            String username = KvStorageElasticsearchUsername.value();
            if (!Strings.isNullOrEmpty(username)) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, KvStorageElasticsearchPassword.value()));
                restClientBuilder = restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            _restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        } catch (IllegalArgumentException e) {
            s_logger.error("Failed to create ElasticSearch client", e);
            return false;
        }
        return true;
    }

    @Override
    public RestHighLevelClient getEsClient() {
        return _restHighLevelClient;
    }
}
