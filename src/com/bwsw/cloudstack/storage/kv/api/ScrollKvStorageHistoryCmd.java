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
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.exception.InsufficientCapacityException;
import com.cloud.exception.NetworkRuleConflictException;
import com.cloud.exception.ResourceAllocationException;
import com.cloud.exception.ResourceUnavailableException;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.context.CallContext;

import javax.inject.Inject;

@APICommand(name = ScrollKvStorageHistoryCmd.API_NAME, description = "Scrolls history records", responseObject = KvHistoryResult.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = false, responseView = ResponseObject.ResponseView.Restricted,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User})
public class ScrollKvStorageHistoryCmd extends BaseCmd {

    public static final String API_NAME = "scrollKvStorageHistory";

    @Parameter(name = ApiConstants.SCROLL_ID, required = true, type = CommandType.STRING, length = Integer.MAX_VALUE,
            description = "the tag to request next batch of history records")
    private String scrollId;

    @Parameter(name = ApiConstants.TIMEOUT, required = true, type = CommandType.LONG, description = "timeout in ms for subsequent scroll requests")
    private Long timeout;

    @Inject
    private KvStorageManager _kvStorageManager;

    public String getScrollId() {
        return scrollId;
    }

    public Long getTimeout() {
        return timeout;
    }

    @Override
    public void execute() throws ResourceUnavailableException, InsufficientCapacityException, ServerApiException, ConcurrentOperationException, ResourceAllocationException,
            NetworkRuleConflictException {
        KvHistoryResult response = _kvStorageManager.getHistory(getScrollId(), getTimeout());
        response.setResponseName(getCommandName());
        setResponseObject(response);
    }

    @Override
    public String getCommandName() {
        return API_NAME.toLowerCase() + BaseCmd.RESPONSE_SUFFIX;
    }

    @Override
    public long getEntityOwnerId() {
        return CallContext.current().getCallingAccount().getAccountId();
    }
}
