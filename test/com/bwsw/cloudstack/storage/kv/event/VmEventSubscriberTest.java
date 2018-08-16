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

import com.bwsw.cloudstack.storage.kv.entity.KvStorage;
import com.bwsw.cloudstack.storage.kv.service.KvStorageManager;
import com.cloud.domain.Domain;
import com.cloud.event.EventCategory;
import com.cloud.event.EventTypes;
import com.cloud.exception.InvalidParameterValueException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.framework.events.EventSubscriber;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class VmEventSubscriberTest extends BaseEventSubscriberTest {

    @Mock
    private KvStorageManager _kvStorageManager;

    @InjectMocks
    private VmEventSubscriber _vmEventSubscriber;

    @Test
    public void testOnEventVmCreateEvent() throws JsonProcessingException {
        testOnEventVmCreate(() -> when(_kvStorageManager.createVmStorage(_event.getResourceUUID())).thenReturn(new KvStorage()));
    }

    @Test
    public void testOnEventVmCreateFailure() throws JsonProcessingException {
        testOnEventVmCreate(() -> when(_kvStorageManager.createVmStorage(_event.getResourceUUID())).thenThrow(new ServerApiException()));
    }

    @Test
    public void testOnEventVmCreateNotCompleted() throws JsonProcessingException {
        testOnEventIgnoredState(EventTypes.EVENT_VM_CREATE, _kvStorageManager);
    }

    @Test
    public void testOnEventVmCreateEventFailure() throws JsonProcessingException {
        testOnEventFailure(EventTypes.EVENT_VM_CREATE, _kvStorageManager);
    }

    @Test
    public void testOnEventVmExpunge() throws JsonProcessingException {
        testOnEventVmExpunge(() -> when(_kvStorageManager.deleteVmStorage(_event.getResourceUUID())).thenReturn(true));
    }

    @Test
    public void testOnEventVmExpungeStorageNotDeleted() throws JsonProcessingException {
        testOnEventVmExpunge(() -> when(_kvStorageManager.deleteVmStorage(_event.getResourceUUID())).thenReturn(false));
    }

    @Test
    public void testOnEventVmExpungeFailure() throws JsonProcessingException {
        testOnEventVmExpunge(() -> when(_kvStorageManager.deleteVmStorage(_event.getResourceUUID())).thenThrow(new InvalidParameterValueException("invalid type")));
    }

    @Test
    public void testOnEventVmExpungeNotCompleted() throws JsonProcessingException {
        testOnEventIgnoredState(EventTypes.EVENT_VM_EXPUNGE, _kvStorageManager);
    }

    @Test
    public void testOnEventVmExpungeEventFailure() throws JsonProcessingException {
        testOnEventFailure(EventTypes.EVENT_VM_EXPUNGE, _kvStorageManager);
    }

    @Test
    public void testOnEventNullResourceUuid() {
        when(_event.getEventSource()).thenReturn(null);

        verifyZeroInteractions(_kvStorageManager);
    }

    @Test
    public void testOnEventUnexpectedEventCategory() {
        when(_event.getEventSource()).thenReturn(UUID);
        when(_event.getEventCategory()).thenReturn(EventCategory.RESOURCE_STATE_CHANGE_EVENT.getName());

        verifyZeroInteractions(_kvStorageManager);
    }

    @Test
    public void testOnEventUnexpectedResourceType() {
        when(_event.getEventSource()).thenReturn(UUID);
        when(_event.getEventCategory()).thenReturn(_vmEventSubscriber.getEventCategory().getName());
        when(_event.getResourceType()).thenReturn(Domain.class.getSimpleName());

        verifyZeroInteractions(_kvStorageManager);
    }

    @Test
    public void testOnEventUnexpectedEventType() {
        when(_event.getResourceUUID()).thenReturn(UUID);
        when(_event.getEventCategory()).thenReturn(_vmEventSubscriber.getEventCategory().getName());
        when(_event.getResourceType()).thenReturn(_vmEventSubscriber.getResourceType());
        when(_event.getEventType()).thenReturn(EventTypes.EVENT_VM_REBOOT);
    }

    @Override
    protected void expectEvent(String eventType, com.cloud.event.Event.State state, String description) throws JsonProcessingException {
        expectEvent(eventType, _vmEventSubscriber.getEventCategory(), _vmEventSubscriber.getResourceType(), state, description);
    }

    @Override
    protected EventSubscriber getEventSubscriber() {
        return _vmEventSubscriber;
    }

    private void testOnEventVmCreate(Expectation expectation) throws JsonProcessingException {
        expectEvent(EventTypes.EVENT_VM_CREATE, com.cloud.event.Event.State.Completed, SUCCESS_DESCRIPTION);
        expectation.apply();

        _vmEventSubscriber.onEvent(_event);

        verify(_kvStorageManager, only()).createVmStorage(_event.getResourceUUID());
    }

    private void testOnEventVmExpunge(Expectation expectation) throws JsonProcessingException {
        expectEvent(EventTypes.EVENT_VM_EXPUNGE, com.cloud.event.Event.State.Completed, SUCCESS_DESCRIPTION);
        expectation.apply();

        _vmEventSubscriber.onEvent(_event);

        verify(_kvStorageManager, only()).deleteVmStorage(_event.getResourceUUID());
    }
}
