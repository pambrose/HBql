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

package org.apache.hadoop.hbase.hbql.util;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionQueue<T> {

    private final AtomicInteger completionCounter = new AtomicInteger(0);
    private final QueueElement<T> completionToken = QueueElement.newCompletionToken();
    private final BlockingQueue<QueueElement<T>> blockingQueue;

    public CompletionQueue(final int size) {
        this.blockingQueue = new ArrayBlockingQueue<QueueElement<T>>(size, true);
    }

    private BlockingQueue<QueueElement<T>> getBlockingQueue() {
        return this.blockingQueue;
    }

    private AtomicInteger getCompletionCounter() {
        return this.completionCounter;
    }

    private QueueElement<T> getCompletionToken() {
        return this.completionToken;
    }

    public int getCompletionCount() {
        return this.getCompletionCounter().get();
    }

    public void putElement(final T val) throws HBqlException {
        final QueueElement<T> element = QueueElement.newElement(val);
        try {
            this.getBlockingQueue().put(element);
        }
        catch (InterruptedException e) {
            throw new HBqlException(e);
        }
    }

    public void putCompletionToken() {
        try {
            this.getBlockingQueue().put(this.getCompletionToken());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public QueueElement<T> takeElement() throws HBqlException {
        try {
            final QueueElement<T> queueElement = this.getBlockingQueue().take();
            if (queueElement.isCompletionToken())
                this.getCompletionCounter().incrementAndGet();
            return queueElement;
        }
        catch (InterruptedException e) {
            throw new HBqlException(e);
        }
    }

    public void reset() {
        this.getCompletionCounter().set(0);
        this.getBlockingQueue().clear();
    }
}
