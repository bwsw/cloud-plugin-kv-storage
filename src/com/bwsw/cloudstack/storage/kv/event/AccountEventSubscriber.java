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
import com.cloud.user.Account;
import org.apache.cloudstack.framework.events.Event;

public class AccountEventSubscriber extends BaseEventSubscriber {

    private final KvStorageManager _kvStorageManager;

    public AccountEventSubscriber(KvStorageManager kvStorageManager) {
        if (kvStorageManager == null) {
            throw new IllegalArgumentException("Null storage manager");
        }
        _kvStorageManager = kvStorageManager;
    }

    @Override
    public EventCategory getEventCategory() {
        return EventCategory.ACTION_EVENT;
    }

    @Override
    public String[] getEventTypes() {
        return new String[] {EventTypes.EVENT_ACCOUNT_DELETE};
    }

    @Override
    public String getResourceType() {
        return Account.class.getSimpleName();
    }

    @Override
    public void onEvent(Event event) {
        if (event.getResourceUUID() != null && getEventCategory().equals(EventCategory.getEventCategory(event.getEventCategory())) && getResourceType()
                .equals(event.getResourceType())) {
            if (EventTypes.EVENT_ACCOUNT_DELETE.equals(event.getEventType()) && isExecutionRequired(event)) {
                try {
                    _kvStorageManager.deleteAccountStorages(event.getResourceUUID());
                    _logger.info("KV storages for the account " + event.getResourceUUID() + " have been deleted");
                } catch (Exception e) {
                    _logger.error("Unable to delete KV storages for the account " + event.getResourceUUID(), e);
                }
            }
        }
    }
}
