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
import org.apache.hadoop.hbase.hbql.client.QueryFuture;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class QueryFutureImpl implements QueryFuture {

    private final CountDownLatch latch = new CountDownLatch(1);
    private HBqlException caughtException = null;
    private long startTime = -1L;
    private long completeTime = -1L;

    public Exception getCaughtException() {
        return this.caughtException;
    }

    public void setCaughtException(final HBqlException exception) {
        this.caughtException = exception;
    }

    protected void markQueryStart() {
        this.startTime = System.currentTimeMillis();
    }

    protected void markQueryComplete() {
        this.completeTime = System.currentTimeMillis();
        this.getLatch().countDown();
    }

    public boolean isStarted() {
        return this.startTime != -1L;
    }

    public boolean isComplete() {
        return this.completeTime != -1L;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getCompleteTime() {
        return this.completeTime;
    }

    public long getElapsedTime() {
        return this.getCompleteTime() - this.getStartTime();
    }

    private CountDownLatch getLatch() {
        return this.latch;
    }

    public void await() throws InterruptedException {
        getLatch().await();
    }

    public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
        return getLatch().await(timeout, unit);
    }
}
