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
import com.cloud.event.Event;
import com.cloud.event.EventTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.framework.events.EventSubscriber;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

public class AccountEventSubscriberTest extends BaseEventSubscriberTest {

    @Mock
    private KvStorageManager _kvStorageManager;

    @InjectMocks
    private AccountEventSubscriber _accountEventSubscriber;

    @Test
    public void testOnEventAccountDeleteEvent() throws JsonProcessingException {
        testOnEventAccountDelete(() -> doNothing().when(_kvStorageManager).deleteAccountStorages(UUID));
    }

    @Test
    public void testOnEventAccountDeleteFailure() throws JsonProcessingException {
        testOnEventAccountDelete(() -> doThrow(new ServerApiException()).when(_kvStorageManager).deleteAccountStorages(UUID));
    }

    @Test
    public void testOnEventAccountDeleteNotCompleted() throws JsonProcessingException {
        testOnEventIgnoredState(com.cloud.event.EventTypes.EVENT_ACCOUNT_DELETE, _kvStorageManager);
    }

    @Test
    public void testOnEventAccountDeleteEventFailure() throws JsonProcessingException {
        testOnEventFailure(EventTypes.EVENT_ACCOUNT_DELETE, _kvStorageManager);
    }

    @Override
    protected void expectEvent(String eventType, Event.State state, String description) throws JsonProcessingException {
        expectEvent(eventType, _accountEventSubscriber.getEventCategory(), _accountEventSubscriber.getResourceType(), state, description);
    }

    @Override
    protected EventSubscriber getEventSubscriber() {
        return _accountEventSubscriber;
    }

    private void testOnEventAccountDelete(Expectation expectation) throws JsonProcessingException {
        expectEvent(EventTypes.EVENT_ACCOUNT_DELETE, com.cloud.event.Event.State.Completed, SUCCESS_DESCRIPTION);
        expectation.apply();

        _accountEventSubscriber.onEvent(_event);

        verify(_kvStorageManager, only()).deleteAccountStorages(UUID);
    }
}
