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

package org.apache.hadoop.hbase.hbql.client;

import java.util.List;

public interface HStatement {

    // Async queries
    QueryFuture executeQueryAsync(String sql, QueryListener<HRecord>... listeners) throws HBqlException;

    <T> QueryFuture executeQueryAsync(String sql, Class clazz, QueryListener<T>... listeners) throws HBqlException;

    // Sync queries
    HResultSet<HRecord> executeQuery(String sql, QueryListener<HRecord>... listeners) throws HBqlException;

    <T> HResultSet<T> executeQuery(String sql, Class clazz, QueryListener<T>... listeners) throws HBqlException;

    ExecutionResults execute(String sql) throws HBqlException;

    List<HRecord> executeQueryAndFetch(String sql, QueryListener<HRecord>... listeners) throws HBqlException;

    <T> List<T> executeQueryAndFetch(String sql, Class clazz, QueryListener<T>... listeners) throws HBqlException;

    ExecutionResults executeUpdate(String sql) throws HBqlException;

    <T> HResultSet<T> getResultSet();

    void close() throws HBqlException;

    boolean isClosed();
}
