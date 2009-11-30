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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HConnectionPool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class HConnectionPoolImpl implements HConnectionPool {

    private final String name;
    private final HBaseConfiguration config;
    private final BlockingQueue<HConnection> connectionQueue;
    private final int poolSize;
    private volatile int count = 0;

    public HConnectionPoolImpl(final int poolSize,
                               final String name,
                               final HBaseConfiguration config) throws HBqlException {
        this.name = name;
        this.config = (config == null) ? new HBaseConfiguration() : config;
        this.poolSize = poolSize;
        this.connectionQueue = new ArrayBlockingQueue<HConnection>(poolSize);
    }

    public String getName() {
        return this.name;
    }

    public HBaseConfiguration getConfig() {
        return this.config;
    }

    private BlockingQueue<HConnection> getConnectionQueue() {
        return this.connectionQueue;
    }

    private int getPoolSize() {
        return this.poolSize;
    }

    private int getCount() {
        return this.count;
    }

    public synchronized HConnection getConnection() throws HBqlException {

        //  Grow the pool as necessary, rather than front-loading it.
        if (this.getConnectionQueue().size() == 0 && this.getCount() < this.getPoolSize()) {
            final HConnectionImpl connection = new HConnectionImpl(this.getConfig(), this);
            this.getConnectionQueue().add(connection);
            this.count++;
        }

        try {
            return this.getConnectionQueue().take();
        }
        catch (InterruptedException e) {
            throw new HBqlException("InterruptedException: " + e.getMessage());
        }
    }

    public void release(final HConnection connection) {
        this.getConnectionQueue().add(connection);
    }
}