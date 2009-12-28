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

package org.apache.expreval.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingQueueWithCompletion<T> {

    private final BlockingQueue<QueueElement<T>> resultQueue;
    private final AtomicInteger completionCounter = new AtomicInteger(0);

    public BlockingQueueWithCompletion(final int size) {
        resultQueue = new ArrayBlockingQueue<QueueElement<T>>(size, true);
    }

    private BlockingQueue<QueueElement<T>> getResultQueue() {
        return this.resultQueue;
    }

    private AtomicInteger getCompletionCounter() {
        return this.completionCounter;
    }

    public int getCompletionCount() {
        return this.getCompletionCounter().get();
    }

    public void putElement(final T val) throws InterruptedException {
        final QueueElement<T> element = QueueElement.newElement(val);
        this.getResultQueue().put(element);
    }

    public void markCompletion() throws InterruptedException {
        final QueueElement<T> element = QueueElement.newComplete();
        this.getResultQueue().put(element);
    }

    public QueueElement<T> takeElement() throws InterruptedException {
        final QueueElement<T> queueElement = this.getResultQueue().take();
        if (queueElement.isCompleteToken())
            this.getCompletionCounter().incrementAndGet();

        return queueElement;
    }

    public void reset() {
        this.getCompletionCounter().set(0);
        this.getResultQueue().clear();
    }
}
