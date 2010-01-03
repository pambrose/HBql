/*
 * Copyright (c) 2010.  The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.util.Maps;

import java.util.Map;
import java.util.NavigableMap;

public abstract class ValueMap<T> extends Value {

    private final Map<String, CurrentAndVersionValue<T>> currentAndVersionMap = Maps.newHashMap();

    protected ValueMap(final String name) throws HBqlException {
        super(name);
    }

    public Map<String, CurrentAndVersionValue<T>> getCurrentAndVersionMap() {
        return this.currentAndVersionMap;
    }

    private CurrentAndVersionValue<T> getMapValue(final String mapKey) throws HBqlException {

        CurrentAndVersionValue<T> hvalue = this.getCurrentAndVersionMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new CurrentAndVersionValue<T>(null);
            this.getCurrentAndVersionMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setCurrentValueMap(final long timestamp, final String mapKey, final T val) throws HBqlException {
        this.getMapValue(mapKey).setCurrentValue(timestamp, val);
    }

    /*
    public Map<Long, T> getVersionMap(final String name, final boolean createIfNull) throws HBqlException {
        return this.getMapValue(name).getVersionMap(createIfNull);
    }
    */
    public void setVersionMap(final String name, final NavigableMap<Long, T> val) throws HBqlException {
        this.getMapValue(name).setVersionMap(val);
    }
}