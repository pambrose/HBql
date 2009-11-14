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
import org.apache.hadoop.hbase.MasterNotRunningException;

import java.io.IOException;
import java.util.Set;

public interface HConnection {

    String getName();

    HBaseConfiguration getConfig();

    <T> Query<T> newQuery(String query) throws IOException, HBqlException;

    <T> Query<T> newQuery(String query, Class clazz) throws IOException, HBqlException;

    ExecutionOutput execute(String str) throws HBqlException, IOException;

    PreparedStatement prepare(String str) throws HBqlException;

    org.apache.hadoop.hbase.client.HTable getHTable(String tableName) throws IOException;

    boolean tableExists(String tableName) throws MasterNotRunningException;

    boolean tableEnabled(String tableName) throws IOException;

    void dropTable(String tableName) throws IOException;

    void disableTable(String tableName) throws IOException;

    void enableTable(String tableName) throws IOException;

    Set<String> getTableNames() throws IOException;

    Set<String> getFamilyNames(String tableName) throws HBqlException;

    void apply(Batch batch) throws IOException;

    void close() throws HBqlException;

    boolean isClosed() throws HBqlException;
}
