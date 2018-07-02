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

import com.bwsw.cloudstack.storage.kv.event.EventTypes;
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.user.Account;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiErrorCode;
import org.apache.cloudstack.api.BaseAsyncCmd;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.SuccessResponse;
import org.apache.cloudstack.context.CallContext;

import javax.inject.Inject;

@APICommand(name = DeleteTempKvStorageCmd.API_NAME, description = "Deletes a temporal KV storage", responseObject = SuccessResponse.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = true, responseView = ResponseObject.ResponseView.Full,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User}, entityType = {Account.class})
public class DeleteTempKvStorageCmd extends BaseAsyncCmd {

    public static final String API_NAME = "deleteTempKvStorage";

    @Parameter(name = com.bwsw.cloudstack.storage.kv.api.ApiConstants.STORAGE_ID, type = BaseCmd.CommandType.STRING, required = true, description = "the KV storage id")
    private String storageId;

    @Inject
    private KvStorageManager _kvStorageManager;

    public String getStorageId() {
        return storageId;
    }

    @Override
    public long getEntityOwnerId() {
        return CallContext.current().getCallingAccount().getAccountId();
    }

    @Override
    public void execute() throws ServerApiException, ConcurrentOperationException {
        boolean result = _kvStorageManager.deleteTempStorage(getStorageId());
        if (result) {
            SuccessResponse response = new SuccessResponse(getCommandName());
            setResponseObject(response);
        } else {
            throw new ServerApiException(ApiErrorCode.INTERNAL_ERROR, "Failed to delete temporal KV storage");
        }
    }

    @Override
    public String getCommandName() {
        return API_NAME.toLowerCase() + BaseCmd.RESPONSE_SUFFIX;
    }

    @Override
    public String getEventType() {
        return EventTypes.EVENT_KV_STORAGE_DELETE;
    }

    @Override
    public String getEventDescription() {
        return "deleting temp kv storage: " + getStorageId();
    }

}
