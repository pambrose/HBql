/*
 * Copyright (c) 2011.  The Apache Software Foundation
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
import org.apache.hadoop.hbase.hbql.util.ArrayBlockingQueues;
import org.apache.hadoop.hbase.hbql.util.PoolableElement;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ElementPool<T extends PoolableElement> {

    private final AtomicInteger createdElementCounter = new AtomicInteger(0);
    private final AtomicInteger takenElementCounter   = new AtomicInteger(0);
    private final BlockingQueue<T> elementPool;
    private final String           name;
    private final int              maxPoolSize;

    public ElementPool(final String name, final int maxPoolSize) {
        this.name = name;
        this.maxPoolSize = maxPoolSize;
        this.elementPool = ArrayBlockingQueues.newArrayBlockingQueue(this.getMaxPoolSize());
    }

    protected abstract T newElement() throws HBqlException;

    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    protected BlockingQueue<T> getElementPool() {
        return this.elementPool;
    }

    public String getName() {
        return this.name;
    }

    private AtomicInteger getCreatedElementCounter() {
        return this.createdElementCounter;
    }

    private AtomicInteger getTakenElementCounter() {
        return this.takenElementCounter;
    }

    public int getCreatedCount() {
        return this.getCreatedElementCounter().get();
    }

    public int getTakenCount() {
        return this.getTakenElementCounter().get();
    }

    protected void addElementToPool() throws HBqlException {
        if (this.getCreatedElementCounter().get() < this.getMaxPoolSize()) {
            final T val = this.newElement();
            this.getElementPool().add(val);
            this.getCreatedElementCounter().incrementAndGet();
        }
    }

    protected synchronized T take() throws HBqlException {

        // Grow the pool as necessary, rather than front-loading it.
        if (this.getElementPool().size() == 0)
            this.addElementToPool();

        try {
            final T retval = this.getElementPool().take();
            retval.resetElement();
            this.getTakenElementCounter().incrementAndGet();
            return retval;
        }
        catch (InterruptedException e) {
            throw new HBqlException("InterruptedException: " + e.getMessage());
        }
    }

    public void release(final T element) {
        element.resetElement();
        this.getElementPool().add(element);
        this.getTakenElementCounter().decrementAndGet();
    }
}
