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

package com.bwsw.cloudstack.storage.kv.api;

import com.bwsw.cloudstack.storage.kv.response.KvHistoryResult;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;

import java.util.List;

@APICommand(name = GetKvStorageHistoryCmd.API_NAME, description = "Get history records", responseObject = KvHistoryResult.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = false, responseView = ResponseObject.ResponseView.Restricted,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User})
public class GetKvStorageHistoryCmd extends BaseKvStorageCmd {

    public static final String API_NAME = "getKvStorageHistory";

    @Parameter(name = ApiConstants.KEYS, required = false, type = CommandType.LIST, collectionType = CommandType.STRING, description = "keys to search history records for")
    private List<String> keys;

    @Parameter(name = ApiConstants.OPERATIONS, required = false, type = CommandType.LIST, collectionType = CommandType.STRING,
            description = "operations to search history records for. Possible values are set, delete and clear.")
    private List<String> operations;

    @Parameter(name = ApiConstants.START, required = false, type = CommandType.LONG,
            description = "the start date/time as Unix timestamp in ms to retrieve history records with dates >= start")
    private Long start;

    @Parameter(name = ApiConstants.START, required = false, type = CommandType.LONG,
            description = "the end date/time as Unix timestamp in ms to retrieve history records with dates <= end")
    private Long end;

    @Parameter(name = ApiConstants.SORT, required = false, type = CommandType.LIST, collectionType = CommandType.STRING,
            description = "response fields optionally prefixed with - (minus) for descending order")
    private List<String> sort;

    @Parameter(name = ApiConstants.PAGE, required = false, type = CommandType.INTEGER, description = "a page number of results (1 by default)")
    private Integer page;

    @Parameter(name = ApiConstants.SIZE, required = false, type = CommandType.INTEGER, description = "a number of results returned in the page/batch")
    private Integer size;

    @Parameter(name = ApiConstants.SCROLL, required = false, type = CommandType.LONG, description = "a timeout in ms for subsequent history scroll requests")
    private Long scroll;

    public List<String> getKeys() {
        return keys;
    }

    public List<String> getOperations() {
        return operations;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public List<String> getSort() {
        return sort;
    }

    public Integer getPage() {
        return page;
    }

    public Integer getSize() {
        return size;
    }

    public Long getScroll() {
        return scroll;
    }

    @Override
    protected KvOperationResponse getResponse() {
        return _kvStorageManager.getHistory(getStorageId(), getKeys(), getOperations(), getStart(), getEnd(), getSort(), getPage(), getSize(), getScroll());
    }

    @Override
    public String getCommandName() {
        return API_NAME.toLowerCase() + BaseCmd.RESPONSE_SUFFIX;
    }
}
