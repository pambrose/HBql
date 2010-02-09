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

package org.apache.hadoop.hbase.jdbc.impl;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetMetaDataImpl implements ResultSetMetaData {

    private final ResultSetImpl resultSet;

    public ResultSetMetaDataImpl(final ResultSetImpl resultSet) {
        this.resultSet = resultSet;
    }

    private ResultSetImpl getResultSet() {
        return this.resultSet;
    }

    public int getColumnCount() throws SQLException {
        return 0;
    }

    public boolean isAutoIncrement(final int i) throws SQLException {
        return false;
    }

    public boolean isCaseSensitive(final int i) throws SQLException {
        return false;
    }

    public boolean isSearchable(final int i) throws SQLException {
        return false;
    }

    public boolean isCurrency(final int i) throws SQLException {
        return false;
    }

    public int isNullable(final int i) throws SQLException {
        return 0;
    }

    public boolean isSigned(final int i) throws SQLException {
        return false;
    }

    public int getColumnDisplaySize(final int i) throws SQLException {
        return 0;
    }

    public String getColumnLabel(final int i) throws SQLException {
        return "";
    }

    public String getColumnName(final int i) throws SQLException {
        return null;
    }

    public String getSchemaName(final int i) throws SQLException {
        return "";
    }

    public String getTableName(final int i) throws SQLException {
        return "";
    }

    public String getCatalogName(final int i) throws SQLException {
        return "";
    }

    public int getColumnType(final int i) throws SQLException {
        return 0;
    }

    public String getColumnTypeName(final int i) throws SQLException {
        return null;
    }

    public int getPrecision(final int i) throws SQLException {
        return 0;
    }

    public int getScale(final int i) throws SQLException {
        return 0;
    }

    public boolean isReadOnly(final int i) throws SQLException {
        return false;
    }

    public boolean isWritable(final int i) throws SQLException {
        return true;
    }

    public boolean isDefinitelyWritable(final int i) throws SQLException {
        return true;
    }

    public String getColumnClassName(final int i) throws SQLException {
        return null;
    }

    public <T> T unwrap(final Class<T> tClass) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> aClass) throws SQLException {
        return false;
    }
}
