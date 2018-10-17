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

package com.bwsw.cloudstack.storage.kv.response;

import com.bwsw.cloudstack.storage.kv.entity.EntityConstants;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.annotations.SerializedName;
import org.apache.cloudstack.api.BaseResponse;

import java.util.Objects;

public class KvHistory extends BaseResponse {

    @JsonProperty
    @SerializedName(EntityConstants.KEY)
    private String key;

    @JsonProperty
    @SerializedName(EntityConstants.VALUE)
    private String value;

    @JsonProperty
    @SerializedName(EntityConstants.OPERATION)
    private String operation;

    @JsonProperty
    @SerializedName(EntityConstants.TIMESTAMP)
    private Long timestamp;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, operation, timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        KvHistory other = (KvHistory)obj;
        return Objects.equals(key, other.key) && Objects.equals(value, other.value) && Objects.equals(operation, other.operation) && Objects.equals(timestamp, other.timestamp);
    }
}
