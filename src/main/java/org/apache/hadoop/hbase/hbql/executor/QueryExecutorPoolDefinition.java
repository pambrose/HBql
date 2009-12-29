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

package org.apache.hadoop.hbase.hbql.executor;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPoolManager;

import java.util.List;

public class QueryExecutorPoolDefinition {

    private final String poolName;
    private final List<ExecutorPoolProperty> executorPoolPropertyList;

    private IntegerExecutorPoolProperty maxPoolSize = null;
    private IntegerExecutorPoolProperty threadCount = null;
    private BooleanExecutorPoolProperty threadsReadResults = null;
    private IntegerExecutorPoolProperty queueSize = null;

    public QueryExecutorPoolDefinition(final String poolName, final List<ExecutorPoolProperty> executorPropertyList) {
        this.poolName = poolName;
        this.executorPoolPropertyList = executorPropertyList;
    }

    public String getPoolName() {
        return this.poolName;
    }

    private List<ExecutorPoolProperty> getExecutorPoolPropertyList() {
        return this.executorPoolPropertyList;
    }

    private ExecutorPoolProperty validateProperty(final ExecutorPoolProperty assignee,
                                                  final ExecutorPoolProperty value) throws HBqlException {
        if (assignee != null)
            throw new HBqlException("Multiple " + value.getPropertyType().getDescription()
                                    + " values for " + this.getPoolName() + " not allowed");
        return value;
    }

    public void validateExecutorPoolPropertyList() throws HBqlException {

        if (this.getExecutorPoolPropertyList() == null)
            return;

        for (final ExecutorPoolProperty executorPoolProperty : this.getExecutorPoolPropertyList()) {

            executorPoolProperty.validate();

            switch (executorPoolProperty.getEnumType()) {

                case MAXPOOLSIZE:
                    this.maxPoolSize = (IntegerExecutorPoolProperty)this.validateProperty(this.maxPoolSize, executorPoolProperty);
                    break;

                case THREADCOUNT:
                    this.threadCount = (IntegerExecutorPoolProperty)this.validateProperty(this.threadCount, executorPoolProperty);
                    break;

                case THREADSREADRESULTS:
                    this.threadsReadResults = (BooleanExecutorPoolProperty)this.validateProperty(this.threadsReadResults, executorPoolProperty);
                    break;

                case QUEUESIZE:
                    this.queueSize = (IntegerExecutorPoolProperty)this.validateProperty(this.queueSize, executorPoolProperty);
                    break;
            }
        }
    }

    public int getMaxPoolSize() throws HBqlException {
        if (this.maxPoolSize != null)
            return this.maxPoolSize.getValue();
        else
            return QueryExecutorPoolManager.defaultMaxPoolSize;
    }

    public int getThreadCount() throws HBqlException {
        if (this.threadCount != null)
            return this.threadCount.getValue();
        else
            return QueryExecutorPoolManager.defaultThreadCount;
    }

    public boolean getThreadsReadResults() throws HBqlException {
        if (this.threadsReadResults != null)
            return this.threadsReadResults.getValue();
        else
            return QueryExecutorPoolManager.defaultThreadsReadResults;
    }

    public int getQueueSize() throws HBqlException {
        if (this.queueSize != null)
            return this.queueSize.getValue();
        else
            return QueryExecutorPoolManager.defaultQueueSize;
    }

    public String asString() {
        try {
            return "MAX_POOL_SIZE : " + this.getMaxPoolSize()
                   + ", THREAD_COUNT : " + this.getThreadCount()
                   + ", THREADS_READ_RESULTS : " + this.getThreadsReadResults()
                   + ", QUEUE_SIZE : " + this.getQueueSize();
        }
        catch (HBqlException e) {
            return "Invalid expression";
        }
    }
}