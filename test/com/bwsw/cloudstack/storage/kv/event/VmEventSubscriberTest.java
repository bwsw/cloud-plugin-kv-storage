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
import org.apache.cloudstack.api.ServerApiException;
import org.apache.cloudstack.framework.events.Event;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VmEventSubscriberTest {

    private static final String UUID = "83e25ab4-700a-4093-a1f5-edb801342ed1";

    private interface Expectation {
        void apply();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private KvStorageManager _kvStorageManager;

    @Mock
    private Event _event;

    @InjectMocks
    private VmEventSubscriber _vmEventSubscriber;

    @Test
    public void testOnEventVmCreate() {
        testOnEventVmCreate(() -> when(_kvStorageManager.createVmStorage(_event.getResourceUUID())).thenReturn(new KvStorage()));
    }

    @Test
    public void testOnEventVmCreateFailure() {
        testOnEventVmCreate(() -> when(_kvStorageManager.createVmStorage(_event.getResourceUUID())).thenThrow(new ServerApiException()));
    }

    @Test
    public void testOnEventVmCreateNotStarted() {
        testOnEventIgnoredState(EventTypes.EVENT_VM_CREATE);
    }

    @Test
    public void testOnEventVmExpunge() {
        testOnEventVmExpunge(() -> when(_kvStorageManager.deleteVmStorage(_event.getResourceUUID())).thenReturn(true));
    }

    @Test
    public void testOnEventVmExpungeStorageNotDeleted() {
        testOnEventVmExpunge(() -> when(_kvStorageManager.deleteVmStorage(_event.getResourceUUID())).thenReturn(true));
    }

    @Test
    public void testOnEventVmExpungeFailure() {
        testOnEventVmExpunge(() -> when(_kvStorageManager.deleteVmStorage(_event.getResourceUUID())).thenThrow(new InvalidParameterValueException("invalid type")));
    }

    @Test
    public void testOnEventVmExpungeNotStarted() {
        testOnEventIgnoredState(EventTypes.EVENT_VM_EXPUNGE);
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
        when(_event.getEventCategory()).thenReturn(VmEventSubscriber.getEventCategory().getName());
        when(_event.getResourceType()).thenReturn(Domain.class.getSimpleName());

        verifyZeroInteractions(_kvStorageManager);
    }

    @Test
    public void testOnEventUnexpectedEventType() {
        when(_event.getResourceUUID()).thenReturn(UUID);
        when(_event.getEventCategory()).thenReturn(VmEventSubscriber.getEventCategory().getName());
        when(_event.getResourceType()).thenReturn(VmEventSubscriber.getResourceType());
        when(_event.getEventType()).thenReturn(EventTypes.EVENT_VM_REBOOT);
    }

    private void testOnEventIgnoredState(String eventType) {
        for (com.cloud.event.Event.State state : com.cloud.event.Event.State.values()) {
            if (state != com.cloud.event.Event.State.Started) {
                expectEvent(eventType, state);
                _vmEventSubscriber.onEvent(_event);
                verifyZeroInteractions(_kvStorageManager);
            }
        }
    }

    private void testOnEventVmCreate(Expectation expectation) {
        expectEvent(EventTypes.EVENT_VM_CREATE, com.cloud.event.Event.State.Started);
        expectation.apply();

        _vmEventSubscriber.onEvent(_event);

        verify(_kvStorageManager).createVmStorage(_event.getResourceUUID());
    }

    private void testOnEventVmExpunge(Expectation expectation) {
        expectEvent(EventTypes.EVENT_VM_EXPUNGE, com.cloud.event.Event.State.Started);
        expectation.apply();

        _vmEventSubscriber.onEvent(_event);

        verify(_kvStorageManager).deleteVmStorage(_event.getResourceUUID());
    }

    private void expectEvent(String eventType, com.cloud.event.Event.State state) {
        when(_event.getResourceUUID()).thenReturn(UUID);
        when(_event.getEventCategory()).thenReturn(VmEventSubscriber.getEventCategory().getName());
        when(_event.getResourceType()).thenReturn(VmEventSubscriber.getResourceType());
        when(_event.getEventType()).thenReturn(eventType);
        when(_event.getDescription()).thenReturn(String.format("{\"status\":\"%s\"}", state));
    }
}
