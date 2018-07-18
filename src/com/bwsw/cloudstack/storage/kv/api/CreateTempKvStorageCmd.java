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

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.event.EventTypes;
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.exception.ConcurrentOperationException;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.BaseAsyncCmd;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.context.CallContext;

import javax.inject.Inject;

@APICommand(name = CreateTempKvStorageCmd.API_NAME, description = "Creates a temporal KV storage", responseObject = KvStorage.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = true, responseView = ResponseObject.ResponseView.Full,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User})
public class CreateTempKvStorageCmd extends BaseAsyncCmd {

    public static final String API_NAME = "createTempKvStorage";

    @Parameter(name = ApiConstants.TTL, type = CommandType.INTEGER, required = true, description = "TTL of the storage to be created")
    private Integer ttl;

    @Inject
    private KvStorageManager _kvStorageManager;

    public Integer getTtl() {
        return ttl;
    }

    @Override
    public void execute() throws ServerApiException, ConcurrentOperationException {
        KvStorage response = _kvStorageManager.createTempStorage(getTtl());
        response.setResponseName(getCommandName());
        response.setObjectName("kvstorage");
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

    @Override
    public String getEventType() {
        return EventTypes.EVENT_KV_STORAGE_CREATE;
    }

    @Override
    public String getEventDescription() {
        return "creating a temporal kv storage";
    }
}
