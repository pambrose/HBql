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

package org.apache.hadoop.hbase.hbql.executor;

import org.apache.hadoop.hbase.hbql.client.AsyncExecutorManager;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.List;

public class AsyncExecutorDefinition {

    private final String name;
    private final List<ExecutorProperty> executorPropertyList;

    private ExecutorProperty minThreadCount = null;
    private ExecutorProperty maxThreadCount = null;
    private ExecutorProperty keepAliveSecs = null;

    public AsyncExecutorDefinition(final String name, final List<ExecutorProperty> executorPropertyList) {
        this.name = name;
        this.executorPropertyList = executorPropertyList;
    }

    public String getName() {
        return this.name;
    }

    private List<ExecutorProperty> getExecutorPropertyList() {
        return this.executorPropertyList;
    }

    private ExecutorProperty validateProperty(final ExecutorProperty assignee,
                                              final ExecutorProperty value) throws HBqlException {
        if (assignee != null)
            throw new HBqlException("Multiple " + value.getPropertyType().getDescription()
                                    + " values for " + this.getName() + " not allowed");
        return value;
    }

    public void validatePropertyList() throws HBqlException {

        if (this.getExecutorPropertyList() == null)
            return;

        for (final ExecutorProperty executorProperty : this.getExecutorPropertyList()) {

            executorProperty.validate();

            switch (executorProperty.getEnumType()) {

                case MIN_THREAD_COUNT:
                    this.minThreadCount = this.validateProperty(this.minThreadCount, executorProperty);
                    break;

                case MAX_THREAD_COUNT:
                    this.maxThreadCount = this.validateProperty(this.maxThreadCount, executorProperty);
                    break;

                case KEEP_ALIVE_SECS:
                    this.keepAliveSecs = this.validateProperty(this.keepAliveSecs, executorProperty);
                    break;
            }
        }
    }

    public int getMinThreadCount() throws HBqlException {
        if (this.minThreadCount != null)
            return this.minThreadCount.getIntegerValue();
        else
            return AsyncExecutorManager.defaultMinThreadCount;
    }

    public int getMaxThreadCount() throws HBqlException {
        if (this.maxThreadCount != null)
            return this.maxThreadCount.getIntegerValue();
        else
            return AsyncExecutorManager.defaultMaxThreadCount;
    }

    public long getKeepAliveSecs() throws HBqlException {
        if (this.keepAliveSecs != null)
            return this.keepAliveSecs.getIntegerValue();
        else
            return AsyncExecutorManager.defaultKeepAliveSecs;
    }

    public String asString() {
        try {
            return " MIN_THREAD_COUNT : " + this.getMinThreadCount()
                   + ", MAX_THREAD_COUNT : " + this.getMaxThreadCount()
                   + ", KEEP_ALIVE_SECS : " + this.getKeepAliveSecs();
        }
        catch (HBqlException e) {
            return "Invalid expression";
        }
    }
}