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

import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class ElementPool<T> {

    private final String name;
    private final int maxPoolSize;
    private final BlockingQueue<T> elementPool;
    private volatile int count = 0;

    public ElementPool(final String name, final int maxPoolSize) {
        this.name = name;
        this.maxPoolSize = maxPoolSize;
        this.elementPool = new ArrayBlockingQueue<T>(this.getMaxPoolSize());
    }

    protected int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    protected BlockingQueue<T> getElementPool() {
        return this.elementPool;
    }

    public String getName() {
        return this.name;
    }

    private int getCount() {
        return this.count;
    }

    protected abstract T newElement() throws HBqlException;

    protected void addElementToPool() throws HBqlException {
        if (this.getCount() < this.getMaxPoolSize()) {
            final T connection = this.newElement();
            this.getElementPool().add(connection);
            this.count++;
        }
    }

    protected synchronized T getElement() throws HBqlException {

        //  Grow the pool as necessary, rather than front-loading it.
        if (this.getElementPool().size() == 0)
            this.addElementToPool();

        try {
            return this.getElementPool().take();
        }
        catch (InterruptedException e) {
            throw new HBqlException("InterruptedException: " + e.getMessage());
        }
    }


    protected void release(final T connection) {
        this.getElementPool().add(connection);
    }
}
