package com.bwsw.cloudstack.storage.kv.entity;

import org.apache.cloudstack.api.ResponseObject;

public interface ResponseEntity extends ResponseObject {

    String getId();

    void setId(String id);
}
