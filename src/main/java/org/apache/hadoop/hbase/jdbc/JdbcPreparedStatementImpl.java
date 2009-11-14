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

package org.apache.hadoop.hbase.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class JdbcPreparedStatementImpl implements PreparedStatement {

    private final JdbcConnectionImpl connection;
    private final String sql;

    public JdbcPreparedStatementImpl(final JdbcConnectionImpl connection, final String sql) {
        this.connection = connection;
        this.sql = sql;
    }

    public <T> T unwrap(final Class<T> tClass) throws SQLException {
        return null;
    }

    public ResultSet executeQuery() throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> aClass) throws SQLException {
        return false;
    }

    public boolean execute(final String s, final int i) throws SQLException {
        return false;
    }

    public int executeUpdate() throws SQLException {
        return 0;
    }

    public void setNull(final int i, final int i1) throws SQLException {

    }

    public void setBoolean(final int i, final boolean b) throws SQLException {

    }

    public ResultSet executeQuery(final String s) throws SQLException {
        return null;
    }

    public void setByte(final int i, final byte b) throws SQLException {

    }

    public int executeUpdate(final String s) throws SQLException {
        return 0;
    }

    public void setShort(final int i, final short i2) throws SQLException {

    }

    public void close() throws SQLException {

    }

    public void setInt(final int i, final int i1) throws SQLException {

    }

    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    public void setLong(final int i, final long l) throws SQLException {

    }

    public void setMaxFieldSize(final int i) throws SQLException {

    }

    public void setFloat(final int i, final float v) throws SQLException {

    }

    public int getMaxRows() throws SQLException {
        return 0;
    }

    public void setDouble(final int i, final double v) throws SQLException {

    }

    public void setMaxRows(final int i) throws SQLException {

    }

    public void setEscapeProcessing(final boolean b) throws SQLException {

    }

    public void setBigDecimal(final int i, final BigDecimal bigDecimal) throws SQLException {

    }

    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    public void setString(final int i, final String s) throws SQLException {

    }

    public void setQueryTimeout(final int i) throws SQLException {

    }

    public void setBytes(final int i, final byte[] bytes) throws SQLException {

    }

    public void cancel() throws SQLException {

    }

    public void setDate(final int i, final Date date) throws SQLException {

    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public void setTime(final int i, final Time time) throws SQLException {

    }

    public void setCursorName(final String s) throws SQLException {

    }

    public void setTimestamp(final int i, final Timestamp timestamp) throws SQLException {

    }

    public boolean execute(final String s) throws SQLException {
        return false;
    }

    public void setAsciiStream(final int i, final InputStream inputStream, final int i1) throws SQLException {

    }

    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    public int getUpdateCount() throws SQLException {
        return 0;
    }

    public void setUnicodeStream(final int i, final InputStream inputStream, final int i1) throws SQLException {

    }

    public boolean getMoreResults() throws SQLException {
        return false;
    }

    public void setFetchDirection(final int i) throws SQLException {

    }

    public void setBinaryStream(final int i, final InputStream inputStream, final int i1) throws SQLException {

    }

    public int getFetchDirection() throws SQLException {
        return 0;
    }

    public void clearParameters() throws SQLException {

    }

    public void setFetchSize(final int i) throws SQLException {

    }

    public void setObject(final int i, final Object o, final int i1) throws SQLException {

    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    public void setObject(final int i, final Object o) throws SQLException {

    }

    public int getResultSetType() throws SQLException {
        return 0;
    }

    public boolean execute() throws SQLException {
        return false;
    }

    public void addBatch(final String s) throws SQLException {

    }

    public void addBatch() throws SQLException {

    }

    public void clearBatch() throws SQLException {

    }

    public void setCharacterStream(final int i, final Reader reader, final int i1) throws SQLException {

    }

    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    public void setRef(final int i, final Ref ref) throws SQLException {

    }

    public Connection getConnection() throws SQLException {
        return null;
    }

    public void setBlob(final int i, final Blob blob) throws SQLException {

    }

    public boolean getMoreResults(final int i) throws SQLException {
        return false;
    }

    public void setClob(final int i, final Clob clob) throws SQLException {

    }

    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    public int executeUpdate(final String s, final int i) throws SQLException {
        return 0;
    }

    public void setArray(final int i, final Array array) throws SQLException {

    }

    public int executeUpdate(final String s, final int[] ints) throws SQLException {
        return 0;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    public int executeUpdate(final String s, final String[] strings) throws SQLException {
        return 0;
    }

    public void setDate(final int i, final Date date, final Calendar calendar) throws SQLException {

    }

    public void setTime(final int i, final Time time, final Calendar calendar) throws SQLException {

    }

    public boolean execute(final String s, final int[] ints) throws SQLException {
        return false;
    }

    public void setTimestamp(final int i, final Timestamp timestamp, final Calendar calendar) throws SQLException {

    }

    public boolean execute(final String s, final String[] strings) throws SQLException {
        return false;
    }

    public void setNull(final int i, final int i1, final String s) throws SQLException {

    }

    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    public void setURL(final int i, final URL url) throws SQLException {

    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void setPoolable(final boolean b) throws SQLException {

    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    public boolean isPoolable() throws SQLException {
        return false;
    }

    public void setRowId(final int i, final RowId rowId) throws SQLException {

    }

    public void setNString(final int i, final String s) throws SQLException {

    }

    public void setNCharacterStream(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void setNClob(final int i, final NClob nClob) throws SQLException {

    }

    public void setClob(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void setBlob(final int i, final InputStream inputStream, final long l) throws SQLException {

    }

    public void setNClob(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void setSQLXML(final int i, final SQLXML sqlxml) throws SQLException {

    }

    public void setObject(final int i, final Object o, final int i1, final int i2) throws SQLException {

    }

    public void setAsciiStream(final int i, final InputStream inputStream, final long l) throws SQLException {

    }

    public void setBinaryStream(final int i, final InputStream inputStream, final long l) throws SQLException {

    }

    public void setCharacterStream(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void setAsciiStream(final int i, final InputStream inputStream) throws SQLException {

    }

    public void setBinaryStream(final int i, final InputStream inputStream) throws SQLException {

    }

    public void setCharacterStream(final int i, final Reader reader) throws SQLException {

    }

    public void setNCharacterStream(final int i, final Reader reader) throws SQLException {

    }

    public void setClob(final int i, final Reader reader) throws SQLException {

    }

    public void setBlob(final int i, final InputStream inputStream) throws SQLException {

    }

    public void setNClob(final int i, final Reader reader) throws SQLException {

    }
}
