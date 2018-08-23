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

import com.bwsw.cloudstack.storage.kv.response.KvKey;
import com.bwsw.cloudstack.storage.kv.response.KvOperationResponse;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;

@APICommand(name = DeleteKvStorageKeyCmd.API_NAME, description = "Removes the mapping by the key", responseObject = KvKey.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = false, responseView = ResponseObject.ResponseView.Restricted,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User})
public class DeleteKvStorageKeyCmd extends BaseKvStorageCmd {

    public static final String API_NAME = "deleteKvStorageKey";

    @Parameter(name = ApiConstants.KEY, type = CommandType.STRING, required = true, description = "the key to be deleted")
    private String key;

    public String getKey() {
        return key;
    }

    @Override
    protected KvOperationResponse getResponse() {
        return _kvStorageManager.deleteKey(getStorageId(), getKey());
    }

    @Override
    public String getCommandName() {
        return API_NAME.toLowerCase() + BaseCmd.RESPONSE_SUFFIX;
    }
}
