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

import java.util.List;
import java.util.Objects;

public class KvHistoryResult extends KvOperationResponse {

    @JsonProperty
    @SerializedName(EntityConstants.TOTAL)
    private Long total;

    @JsonProperty
    @SerializedName(EntityConstants.PAGE)
    private Integer page;

    @JsonProperty
    @SerializedName(EntityConstants.SIZE)
    private Integer size;

    @JsonProperty
    @SerializedName(EntityConstants.SCROLL_ID)
    private String scrollId;

    @JsonProperty
    @SerializedName(EntityConstants.ITEMS)
    private List<KvHistory> items;

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public List<KvHistory> getItems() {
        return items;
    }

    public void setItems(List<KvHistory> items) {
        this.items = items;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, page, size, scrollId, items);
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

        KvHistoryResult other = (KvHistoryResult)obj;
        return Objects.equals(total, other.total) && Objects.equals(page, this.page) && Objects.equals(size, other.size) && Objects.equals(scrollId, other.scrollId) && Objects
                .equals(items, other.items);
    }
}
