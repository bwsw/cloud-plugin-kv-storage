package com.bwsw.cloudstack.storage.kv.api;

import com.bwsw.cloudstack.storage.kv.response.KvStorageResponse;
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.exception.ConcurrentOperationException;
import com.cloud.user.Account;
import org.apache.cloudstack.acl.RoleType;
import org.apache.cloudstack.acl.SecurityChecker;
import org.apache.cloudstack.api.ACL;
import org.apache.cloudstack.api.APICommand;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseCmd;
import org.apache.cloudstack.api.BaseListCmd;
import org.apache.cloudstack.api.Parameter;
import org.apache.cloudstack.api.ResponseObject;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.api.response.AccountResponse;
import org.apache.cloudstack.api.response.ListResponse;

import javax.inject.Inject;

@APICommand(name = ListKvStoragesCmd.API_NAME, description = "Lists KV storages", responseObject = KvStorageResponse.class, requestHasSensitiveInfo = false,
        responseHasSensitiveInfo = true, responseView = ResponseObject.ResponseView.Full,
        authorized = {RoleType.Admin, RoleType.ResourceAdmin, RoleType.DomainAdmin, RoleType.User}, entityType = {Account.class})
public class ListKvStoragesCmd extends BaseListCmd {

    public static final String API_NAME = "listKvStorages";

    @ACL(accessType = SecurityChecker.AccessType.OperateEntry)
    @Parameter(name = ApiConstants.ID, type = CommandType.UUID, entityType = AccountResponse.class, required = true, description = "the ID of the account")
    private Long id;

    @Inject
    private KvStorageManager _kvStorageManager;

    public Long getId() {
        return id;
    }

    @Override
    public long getEntityOwnerId() {
        Account account = _entityMgr.findById(Account.class, getId());
        if (account != null) {
            return account.getAccountId();
        }

        return Account.ACCOUNT_ID_SYSTEM; // no account info given, parent this command to SYSTEM so ERROR events are tracked
    }

    @Override
    public void execute() throws ServerApiException, ConcurrentOperationException {
        ListResponse<KvStorageResponse> response = _kvStorageManager.listStorages(getId(), getStartIndex(), getPageSizeVal());
        response.setResponseName(getCommandName());
        response.setObjectName("kvstorages");
        setResponseObject(response);
    }

    @Override
    public String getCommandName() {
        return API_NAME.toLowerCase() + BaseCmd.RESPONSE_SUFFIX;
    }
}
