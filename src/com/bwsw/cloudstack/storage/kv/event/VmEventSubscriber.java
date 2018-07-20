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
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.cloudstack.framework.events.Event;
import org.apache.cloudstack.framework.events.EventSubscriber;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.Map;

public class VmEventSubscriber implements EventSubscriber {

    private static final Logger s_logger = Logger.getLogger(VmEventSubscriber.class);

    private final KvStorageManager _kvStorageManager;
    private final Gson _gson;
    private final Type _mapType;

    public VmEventSubscriber(KvStorageManager kvStorageManager) {
        if (kvStorageManager == null) {
            throw new IllegalArgumentException("Null storage manager");
        }
        _kvStorageManager = kvStorageManager;
        _gson = new Gson();
        _mapType = new TypeToken<Map<String, String>>() {
        }.getType();
    }

    public static EventCategory getEventCategory() {
        return EventCategory.ACTION_EVENT;
    }

    public static String[] getEventTypes() {
        return new String[] {EventTypes.EVENT_VM_CREATE, EventTypes.EVENT_VM_EXPUNGE, EventTypes.EVENT_VM_START};
    }

    public static String getResourceType() {
        return VirtualMachine.class.getSimpleName();
    }

    @Override
    public void onEvent(Event event) {
        if (event.getResourceUUID() != null && getEventCategory().equals(EventCategory.getEventCategory(event.getEventCategory())) && getResourceType()
                .equals(event.getResourceType())) {
            if (EventTypes.EVENT_VM_CREATE.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    _kvStorageManager.createVmStorage(event.getResourceUUID());
                    s_logger.info("The KV storage for VM " + event.getResourceUUID() + " has been created");
                } catch (Exception e) {
                    s_logger.error("Unable to create the KV storage for VM " + event.getResourceUUID(), e);
                }
            } else if (EventTypes.EVENT_VM_EXPUNGE.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    boolean result = _kvStorageManager.deleteVmStorage(event.getResourceUUID());
                    if (result) {
                        s_logger.info("The KV storage for VM " + event.getResourceUUID() + " has been deleted");
                    } else {
                        s_logger.error("Unable to delete the KV storage for VM " + event.getResourceUUID());
                    }
                } catch (Exception e) {
                    s_logger.error("Unable to delete the KV storage for VM " + event.getResourceUUID(), e);
                }
            } else if (EventTypes.EVENT_VM_START.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    _kvStorageManager.getOrCreateVmStorage(event.getResourceUUID());
                    s_logger.info("The KV storage for VM " + event.getResourceUUID() + " has been created");
                } catch (Exception e) {
                    s_logger.error("Unable to get or create the KV storage for VM " + event.getResourceUUID(), e);
                }
            }
        }

    }

    private boolean isExecutionRequired(Event event) {
        if (!Strings.isNullOrEmpty(event.getDescription())) {
            try {
                Map<String, String> details = _gson.fromJson(event.getDescription(), _mapType);
                if (details.containsKey("status")) {
                    return com.cloud.event.Event.State.valueOf(details.get("status")) == com.cloud.event.Event.State.Started;
                }
            } catch (Exception e) {
                // if the status can not be parsed do nothing
                s_logger.error("Unable to detect the event status: " + event.getDescription());
            }
        }
        return false;
    }
}
