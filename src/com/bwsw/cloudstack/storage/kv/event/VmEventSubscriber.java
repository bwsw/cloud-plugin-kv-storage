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

package com.bwsw.cloudstack.storage.kv.event;

import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.event.EventCategory;
import com.cloud.event.EventTypes;
import com.cloud.vm.VirtualMachine;
import org.apache.cloudstack.framework.events.Event;

public class VmEventSubscriber extends BaseEventSubscriber {

    private final KvStorageManager _kvStorageManager;

    public VmEventSubscriber(KvStorageManager kvStorageManager) {
        if (kvStorageManager == null) {
            throw new IllegalArgumentException("Null storage manager");
        }
        _kvStorageManager = kvStorageManager;
    }

    public EventCategory getEventCategory() {
        return EventCategory.ACTION_EVENT;
    }

    public String[] getEventTypes() {
        return new String[] {EventTypes.EVENT_VM_CREATE, EventTypes.EVENT_VM_EXPUNGE, EventTypes.EVENT_VM_START};
    }

    public String getResourceType() {
        return VirtualMachine.class.getSimpleName();
    }

    @Override
    public void onEvent(Event event) {
        if (event.getResourceUUID() != null && getEventCategory().equals(EventCategory.getEventCategory(event.getEventCategory())) && getResourceType()
                .equals(event.getResourceType())) {
            if (EventTypes.EVENT_VM_CREATE.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    _kvStorageManager.createVmStorage(event.getResourceUUID());
                    _logger.info("The KV storage for VM " + event.getResourceUUID() + " has been created");
                } catch (Exception e) {
                    _logger.error("Unable to create the KV storage for VM " + event.getResourceUUID(), e);
                }
            } else if (EventTypes.EVENT_VM_EXPUNGE.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    boolean result = _kvStorageManager.deleteVmStorage(event.getResourceUUID());
                    if (result) {
                        _logger.info("The KV storage for VM " + event.getResourceUUID() + " has been deleted");
                    } else {
                        _logger.error("Unable to delete the KV storage for VM " + event.getResourceUUID());
                    }
                } catch (Exception e) {
                    _logger.error("Unable to delete the KV storage for VM " + event.getResourceUUID(), e);
                }
            } else if (EventTypes.EVENT_VM_START.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    _kvStorageManager.getOrCreateVmStorage(event.getResourceUUID());
                    _logger.info("The KV storage for VM " + event.getResourceUUID() + " has been created");
                } catch (Exception e) {
                    _logger.error("Unable to get or create the KV storage for VM " + event.getResourceUUID(), e);
                }
            }
        }
    }
}
