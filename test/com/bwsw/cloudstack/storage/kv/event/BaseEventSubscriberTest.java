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

import com.cloud.event.EventCategory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cloudstack.framework.events.Event;
import org.apache.cloudstack.framework.events.EventSubscriber;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public abstract class BaseEventSubscriberTest {

    protected static final String UUID = "83e25ab4-700a-4093-a1f5-edb801342ed1";
    protected static final String SUCCESS_DESCRIPTION = "Successfully completed ";
    protected static final String FAILURE_DESCRIPTION = "Error while ";

    protected interface Expectation {
        void apply();
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    protected Event _event;

    private ObjectMapper objectMapper = new ObjectMapper();

    protected abstract void expectEvent(String eventType, com.cloud.event.Event.State state, String description) throws JsonProcessingException;

    protected abstract EventSubscriber getEventSubscriber();

    protected void expectEvent(String eventType, EventCategory eventCategory, String resourceType, com.cloud.event.Event.State state, String description)
            throws JsonProcessingException {
        when(_event.getResourceUUID()).thenReturn(UUID);
        when(_event.getEventCategory()).thenReturn(eventCategory.getName());
        when(_event.getResourceType()).thenReturn(resourceType);
        when(_event.getEventType()).thenReturn(eventType);
        Map<String, String> eventDescription = new HashMap<>();
        eventDescription.put("status", state.name());
        eventDescription.put("description", description);
        when(_event.getDescription()).thenReturn(objectMapper.writeValueAsString(eventDescription));
    }

    protected void testOnEventIgnoredState(String eventType, Object... mocks) throws JsonProcessingException {
        for (com.cloud.event.Event.State state : com.cloud.event.Event.State.values()) {
            if (state != com.cloud.event.Event.State.Completed) {
                expectEvent(eventType, state, SUCCESS_DESCRIPTION);
                getEventSubscriber().onEvent(_event);
                verifyZeroInteractions(mocks);
            }
        }
    }

    protected void testOnEventFailure(String eventType, Object... mocks) throws JsonProcessingException {
        expectEvent(eventType, com.cloud.event.Event.State.Completed, FAILURE_DESCRIPTION);
        getEventSubscriber().onEvent(_event);
        verifyZeroInteractions(mocks);
    }
}
