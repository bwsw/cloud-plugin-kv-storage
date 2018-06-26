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

import com.bwsw.cloudstack.storage.kv.entity.EntityConstants;
import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.user.Account;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.acl.SecurityChecker;
import org.apache.cloudstack.api.ACL;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.AccountResponse;

import javax.inject.Inject;

@APICommand(name = CreateAccountKvStorageCmd.API_NAME, description = "Creates an account KV storage", responseObject = KvStorage.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = true, responseView = ResponseObject.ResponseView.Full,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User}, entityType = {Account.class})
public class CreateAccountKvStorageCmd extends BaseCmd {

    public static final String API_NAME = "createAccountKvStorage";

    @ACL(accessType = SecurityChecker.AccessType.OperateEntry)
    @Parameter(name = ApiConstants.ACCOUNT_ID, type = CommandType.UUID, entityType = AccountResponse.class, required = true, description = "the ID of the account")
    private Long accountId;

    @Parameter(name = ApiConstants.NAME, type = CommandType.STRING, required = true, description = "the KV storage name")
    private String name;

    @Parameter(name = ApiConstants.DESCRIPTION, type = CommandType.STRING, description = "the KV storage description")
    private String description;

    @Parameter(name = EntityConstants.HISTORY_ENABLED, type = CommandType.BOOLEAN, description = "true if KV storage should keep an operation history, false otherwise")
    private Boolean historyEnabled;

    @Inject
    private KvStorageManager _kvStorageManager;

    public Long getAccountId() {
        return accountId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Boolean getHistoryEnabled() {
        return historyEnabled;
    }

    @Override
    public long getEntityOwnerId() {
        Account account = _entityMgr.findById(Account.class, getAccountId());
        if (account != null) {
            return account.getAccountId();
        }

        return Account.ACCOUNT_ID_SYSTEM; // no account info given, parent this command to SYSTEM so ERROR events are tracked
    }

    @Override
    public void execute() throws ServerApiException, ConcurrentOperationException {
        KvStorage response = _kvStorageManager.createAccountStorage(getAccountId(), getName(), getDescription(), getHistoryEnabled());
        response.setResponseName(getCommandName());
        response.setObjectName("kvstorage");
        setResponseObject(response);
    }

    @Override
    public String getCommandName() {
        return API_NAME.toLowerCase() + BaseCmd.RESPONSE_SUFFIX;
    }
}
