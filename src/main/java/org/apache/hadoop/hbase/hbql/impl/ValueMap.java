/*
 * Copyright (c) 2009.  The Apache Software Foundation
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

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Value;

import java.util.Map;
import java.util.NavigableMap;

public abstract class ValueMap<T> extends Value {

    private final Map<String, CurrentAndVersionValue<T>> currentAndVersionMap = Maps.newHashMap();
    private final Class elementClazz;

    protected ValueMap(final RecordImpl record, final String name, final Class elementClazz) throws HBqlException {
        super(record, name);
        this.elementClazz = elementClazz;
    }

    public Map<String, CurrentAndVersionValue<T>> getCurrentAndVersionMap() {
        return this.currentAndVersionMap;
    }

    private Class getElementClazz() {
        return this.elementClazz;
    }

    public T getCurrentMapValue(final String name, final boolean createIfNull) throws HBqlException {

        final T retval = this.getMapValue(name).getValue();

        if (retval != null || !createIfNull)
            return retval;

        if (this.getElementClazz() == null)
            throw new InternalErrorException();

        final T newVal;
        try {
            newVal = (T)this.getElementClazz().newInstance();
            this.setCurrentValueMap(0, name, newVal);
        }
        catch (InstantiationException e) {
            throw new HBqlException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new HBqlException(e.getMessage());
        }

        return newVal;
    }

    public CurrentAndVersionValue<T> getMapValue(final String mapKey) throws HBqlException {

        CurrentAndVersionValue<T> hvalue = this.getCurrentAndVersionMap().get(mapKey);
        if (hvalue == null) {
            hvalue = new CurrentAndVersionValue<T>(null, null);
            this.getCurrentAndVersionMap().put(mapKey, hvalue);
        }
        return hvalue;
    }

    public void setCurrentValueMap(final long timestamp, final String mapKey, final T val) throws HBqlException {
        this.getMapValue(mapKey).setCurrentValue(timestamp, val);
    }

    public Map<Long, T> getVersionMap(final String name, final boolean createIfNull) throws HBqlException {
        return this.getMapValue(name).getVersionMap(createIfNull);
    }

    public void setVersionMap(final String name, final NavigableMap<Long, T> val) throws HBqlException {
        this.getMapValue(name).setVersionMap(val);
    }
}