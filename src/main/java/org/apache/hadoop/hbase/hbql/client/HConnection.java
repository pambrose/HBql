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

package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Set;

public interface HConnection {

    String getName();

    HBaseConfiguration getConfig();

    //<T> Query<T> newQuery(String query) throws HBqlException;

    //<T> Query<T> newQuery(String query, Class clazz) throws HBqlException;

    ExecutionResults execute(String str) throws HBqlException;

    HStatement createStatement();

    HPreparedStatement prepareStatement(String str) throws HBqlException;

    org.apache.hadoop.hbase.client.HTable getHTable(String tableName) throws HBqlException;

    boolean tableExists(String tableName) throws HBqlException;

    boolean tableEnabled(String tableName) throws HBqlException;

    void dropTable(String tableName) throws HBqlException;

    void disableTable(String tableName) throws HBqlException;

    void enableTable(String tableName) throws HBqlException;

    Set<String> getTableNames() throws HBqlException;

    Set<String> getFamilyNames(String tableName) throws HBqlException;

    void apply(Batch batch) throws HBqlException;

    void close() throws HBqlException;

    boolean isClosed() throws HBqlException;
}
