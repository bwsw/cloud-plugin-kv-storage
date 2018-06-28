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

package com.bwsw.cloudstack.storage.kv.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseResponse;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class KvStorage extends BaseResponse implements ResponseEntity {

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    public enum KvStorageType {
        ACCOUNT, VM, TEMP
    }

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    @SerializedName(ApiConstants.ID)
    private String id;

    private KvStorageType type;

    @SerializedName(ApiConstants.ACCOUNT)
    private String account;

    @SerializedName(ApiConstants.NAME)
    private String name;

    @SerializedName(ApiConstants.DESCRIPTION)
    private String description;

    @SerializedName(com.bwsw.cloudstack.storage.kv.api.ApiConstants.HISTORY_ENABLED)
    private Boolean historyEnabled;

    private Integer ttl;
    private Long expirationTimestamp;

    public KvStorage() {
    }

    public KvStorage(String id, boolean historyEnabled) {
        this.id = id;
        this.type = KvStorageType.VM;
        this.historyEnabled = historyEnabled;
    }

    public KvStorage(String id, String account, String name, String description, boolean historyEnabled) {
        this.id = id;
        this.type = KvStorageType.ACCOUNT;
        this.account = account;
        this.name = name;
        this.description = description;
        this.historyEnabled = historyEnabled;
    }

    public KvStorage(String id, Integer ttl, Long expirationTimestamp) {
        this.id = id;
        this.type = KvStorageType.TEMP;
        this.ttl = ttl;
        this.expirationTimestamp = expirationTimestamp;
        this.historyEnabled = false;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public KvStorageType getType() {
        return type;
    }

    public void setType(KvStorageType type) {
        this.type = type;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty("history_enabled")
    public Boolean getHistoryEnabled() {
        return historyEnabled;
    }

    public void setHistoryEnabled(Boolean historyEnabled) {
        this.historyEnabled = historyEnabled;
    }

    public Integer getTtl() {
        return ttl;
    }

    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    @JsonProperty("expiration_timestamp")
    public Long getExpirationTimestamp() {
        return expirationTimestamp;
    }

    public void setExpirationTimestamp(Long expirationTimestamp) {
        this.expirationTimestamp = expirationTimestamp;
    }
}
