package com.bwsw.cloudstack.storage.kv.response;

import com.google.gson.annotations.SerializedName;
import org.apache.cloudstack.api.ApiConstants;
import org.apache.cloudstack.api.BaseResponse;

public class KvStorageResponse extends BaseResponse {

    @SerializedName(ApiConstants.ID)
    private String id;

    @SerializedName(ApiConstants.NAME)
    private String name;

    @SerializedName(ApiConstants.DESCRIPTION)
    private String description;

    public KvStorageResponse() {
        super("kvstorage");
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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
}
