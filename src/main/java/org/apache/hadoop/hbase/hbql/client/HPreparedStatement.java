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

package org.apache.hadoop.hbase.hbql.client;

import java.util.List;

public interface HPreparedStatement extends HStatement {

    ExecutionResults execute() throws HBqlException;

    HResultSet<HRecord> executeQuery() throws HBqlException;

    <T> HResultSet<T> executeQuery(Class clazz) throws HBqlException;

    List<HRecord> executeQueryAndFetch() throws HBqlException;

    <T> List<T> executeQueryAndFetch(Class clazz) throws HBqlException;

    ExecutionResults executeUpdate() throws HBqlException;

    int setParameter(String name, Object val) throws HBqlException;

    void setParameter(int i, Object obj) throws HBqlException;

    void resetParameters() throws HBqlException;
}
