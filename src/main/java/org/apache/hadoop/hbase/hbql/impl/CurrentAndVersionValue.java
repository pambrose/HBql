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

import org.apache.hadoop.hbase.hbql.util.AtomicReferences;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

public class CurrentAndVersionValue<T> extends Value {

    private final AtomicReference<NavigableMap<Long, T>> atomicVersionMap = AtomicReferences.newAtomicReference();
    private boolean currentValueSet = false;
    private T currentValue = null;
    private long currentValueTimestamp = -1;

    public CurrentAndVersionValue(final String name) {
        super(name);
    }

    public T getCurrentValue() {
        return this.currentValue;
    }

    public boolean isValueSet() {
        return currentValueSet;
    }

    private AtomicReference<NavigableMap<Long, T>> getAtomicVersionMap() {
        return this.atomicVersionMap;
    }

    public void setCurrentValue(final long timestamp, final T val) {
        if (timestamp >= this.currentValueTimestamp) {
            this.currentValueSet = true;
            this.currentValueTimestamp = timestamp;
            this.currentValue = val;
        }
    }

    public NavigableMap<Long, T> getVersionMap() {
        if (this.getAtomicVersionMap().get() == null) {
            synchronized (this) {
                if (this.getAtomicVersionMap().get() == null)
                    this.setVersionMap(new TreeMap<Long, T>());
            }
        }

        return this.getAtomicVersionMap().get();
    }

    public void setVersionMap(final NavigableMap<Long, T> versionMap) {
        this.getAtomicVersionMap().set(versionMap);
    }
}