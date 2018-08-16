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
import org.apache.cloudstack.framework.events.EventBus;
import org.apache.cloudstack.framework.events.EventBusException;
import org.apache.cloudstack.framework.events.EventTopic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EventSubscriberFactoryImplTest {

    private interface SubscriberProducer<T extends BaseEventSubscriber> {
        T get() throws EventBusException;
    }

    @Mock
    private EventBus _eventBus;

    @Mock
    private KvStorageManager _kvStorageManager;

    @InjectMocks
    private EventSubscriberFactoryImpl _eventSubscriberFactory = new EventSubscriberFactoryImpl();

    @Test
    public void testGetVmEventSubscriber() throws EventBusException {
        test(_eventSubscriberFactory::getVmEventSubscriber,
                vmEventSubscriber -> assertSame(_kvStorageManager, ReflectionTestUtils.getField(vmEventSubscriber, "_kvStorageManager")));
    }

    @Test
    public void testGetAccountEventSubscriber() throws EventBusException {
        test(_eventSubscriberFactory::getAccountEventSubscriber,
                accountEventSubscriber -> assertSame(_kvStorageManager, ReflectionTestUtils.getField(accountEventSubscriber, "_kvStorageManager")));
    }

    private <T extends BaseEventSubscriber> void test(SubscriberProducer<T> producer, Consumer<T> validator) throws EventBusException {
        when(_eventBus.subscribe(any(EventTopic.class), any(BaseEventSubscriber.class))).thenAnswer(invocation -> {
            EventTopic eventTopic = invocation.getArgumentAt(0, EventTopic.class);
            BaseEventSubscriber subscriber = invocation.getArgumentAt(1, BaseEventSubscriber.class);
            if (subscriber == null) {
                throw new IllegalArgumentException();
            }
            if (eventTopic == null || !subscriber.getEventCategory().getName().equals(eventTopic.getEventCategory())) {
                throw new IllegalArgumentException();
            }
            boolean validEventType = false;
            for (String eventType : subscriber.getEventTypes()) {
                if (eventType.equals(eventTopic.getEventType())) {
                    validEventType = true;
                    break;
                }
            }
            if (!validEventType) {
                throw new IllegalArgumentException();
            }
            return UUID.randomUUID();
        });

        T subscriber = producer.get();

        assertNotNull(subscriber);
        validator.accept(subscriber);

        verify(_eventBus, times(subscriber.getEventTypes().length)).subscribe(any(EventTopic.class), any(BaseEventSubscriber.class));
    }
}
