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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPoolManager;

import java.util.List;

public class QueryExecutorPoolDefinition {

    private final String poolName;
    private final List<ExecutorProperty> executorPropertyList;

    private ExecutorProperty maxExecutorPoolSize = null;
    private ExecutorProperty minThreadCount = null;
    private ExecutorProperty maxThreadCount = null;
    private ExecutorProperty keepAliveSecs = null;
    private ExecutorProperty threadsReadResults = null;
    private ExecutorProperty completionQueueSize = null;

    public QueryExecutorPoolDefinition(final String poolName, final List<ExecutorProperty> executorPropertyList) {
        this.poolName = poolName;
        this.executorPropertyList = executorPropertyList;
    }

    public String getPoolName() {
        return this.poolName;
    }

    private List<ExecutorProperty> getExecutorPoolPropertyList() {
        return this.executorPropertyList;
    }

    private ExecutorProperty validateProperty(final ExecutorProperty assignee,
                                              final ExecutorProperty value) throws HBqlException {
        if (assignee != null)
            throw new HBqlException("Multiple " + value.getPropertyType().getDescription()
                                    + " values for " + this.getPoolName() + " not allowed");
        return value;
    }

    public void validateExecutorPoolPropertyList() throws HBqlException {

        if (this.getExecutorPoolPropertyList() == null)
            return;

        for (final ExecutorProperty executorProperty : this.getExecutorPoolPropertyList()) {

            executorProperty.validate();

            switch (executorProperty.getEnumType()) {

                case MAX_EXECUTOR_POOL_SIZE:
                    this.maxExecutorPoolSize = this.validateProperty(this.maxExecutorPoolSize, executorProperty);
                    break;

                case MIN_THREAD_COUNT:
                    this.minThreadCount = this.validateProperty(this.minThreadCount, executorProperty);
                    break;

                case MAX_THREAD_COUNT:
                    this.maxThreadCount = this.validateProperty(this.maxThreadCount, executorProperty);
                    break;

                case KEEP_ALIVE_SECS:
                    this.keepAliveSecs = this.validateProperty(this.keepAliveSecs, executorProperty);
                    break;

                case THREADS_READ_RESULTS:
                    this.threadsReadResults = this.validateProperty(this.threadsReadResults, executorProperty);
                    break;

                case COMPLETION_QUEUE_SIZE:
                    this.completionQueueSize = this.validateProperty(this.completionQueueSize, executorProperty);
                    break;
            }
        }
    }

    public int getMaxExecutorPoolSize() throws HBqlException {
        if (this.maxExecutorPoolSize != null)
            return this.maxExecutorPoolSize.getIntegerValue();
        else
            return QueryExecutorPoolManager.defaultMaxExecutorPoolSize;
    }

    public int getMinThreadCount() throws HBqlException {
        if (this.minThreadCount != null)
            return this.minThreadCount.getIntegerValue();
        else
            return QueryExecutorPoolManager.defaultMinThreadCount;
    }

    public int getMaxThreadCount() throws HBqlException {
        if (this.maxThreadCount != null)
            return this.maxThreadCount.getIntegerValue();
        else
            return QueryExecutorPoolManager.defaultMaxThreadCount;
    }

    public long getKeepAliveSecs() throws HBqlException {
        if (this.keepAliveSecs != null)
            return this.keepAliveSecs.getIntegerValue();
        else
            return QueryExecutorPoolManager.defaultKeepAliveSecs;
    }

    public boolean getThreadsReadResults() throws HBqlException {
        if (this.threadsReadResults != null)
            return this.threadsReadResults.getBooleanValue();
        else
            return QueryExecutorPoolManager.defaultThreadsReadResults;
    }

    public int getCompletionQueueSize() throws HBqlException {
        if (this.completionQueueSize != null)
            return this.completionQueueSize.getIntegerValue();
        else
            return QueryExecutorPoolManager.defaultCompletionQueueSize;
    }

    public String asString() {
        try {
            return " MAX_EXECUTOR_POOL_SIZE : " + this.getMaxExecutorPoolSize()
                   + ", MIN_THREAD_COUNT : " + this.getMinThreadCount()
                   + ", MAX_THREAD_COUNT : " + this.getMaxThreadCount()
                   + ", KEEP_ALIVE_SECS : " + this.getKeepAliveSecs()
                   + ", THREADS_READ_RESULTS : " + this.getThreadsReadResults()
                   + ", COMPLETION_QUEUE_SIZE : " + this.getCompletionQueueSize();
        }
        catch (HBqlException e) {
            return "Invalid expression";
        }
    }
}