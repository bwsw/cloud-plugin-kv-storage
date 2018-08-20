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
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.cloudstack.framework.events.Event;
import org.apache.cloudstack.framework.events.EventSubscriber;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.Map;

public abstract class BaseEventSubscriber implements EventSubscriber {

    private static final String STATUS_FIELD = "status";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String ERROR_PREFIX = "Error";

    protected final Logger _logger = Logger.getLogger(getClass());

    private final Gson _gson;
    private final Type _mapType;

    protected BaseEventSubscriber() {
        _gson = new Gson();
        _mapType = new TypeToken<Map<String, String>>() {
        }.getType();
    }

    public abstract EventCategory getEventCategory();

    public abstract String[] getEventTypes();

    public abstract String getResourceType();

    protected boolean isExecutionRequired(Event event) {
        if (!Strings.isNullOrEmpty(event.getDescription())) {
            try {
                Map<String, String> details = _gson.fromJson(event.getDescription(), _mapType);
                if (details.containsKey(STATUS_FIELD)) {
                    return com.cloud.event.Event.State.valueOf(details.get(STATUS_FIELD)) == com.cloud.event.Event.State.Completed && details.containsKey(DESCRIPTION_FIELD)
                            && !StringUtils.startsWith(details.get(DESCRIPTION_FIELD), ERROR_PREFIX);
                }
            } catch (Exception e) {
                // if the status can not be parsed do nothing
                _logger.error("Unable to detect the event status: " + event.getDescription());
            }
        }
        return false;
    }
}
