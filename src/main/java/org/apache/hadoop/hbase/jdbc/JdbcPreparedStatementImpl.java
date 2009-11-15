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

import org.apache.expreval.expr.var.NamedParameter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.statement.HBqlStatement;
import org.apache.hadoop.hbase.hbql.statement.ParameterSupport;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class JdbcPreparedStatementImpl extends JdbcStatementImpl implements PreparedStatement {

    private final HBqlStatement statement;

    public JdbcPreparedStatementImpl(final JdbcConnectionImpl connection, final String sql) throws HBqlException {
        super(connection);
        this.statement = JdbcUtil.parseJdbcStatement(sql);

        if ((this.getStatement() instanceof ParameterSupport)) {
            final ParameterSupport paramStmt = (ParameterSupport)this.getStatement();
            paramStmt.validate(this.getConnectionImpl());
        }
    }

    private HBqlStatement getStatement() {
        return this.statement;
    }

    public ResultSet executeQuery() throws SQLException {
        return this.executeQuery(this.getStatement());
    }

    public int executeUpdate() throws SQLException {
        return this.executeUpdate(this.getStatement());
    }

    public boolean execute() throws SQLException {
        return this.execute(this.getStatement());
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    private void setParameter(final int i, final Object obj) throws HBqlException {
        if (!(this.getStatement() instanceof ParameterSupport)) {
            throw new HBqlException(this.getStatement().getClass().getSimpleName()
                                    + " statements do not support parameters");
        }

        final ParameterSupport paramStmt = (ParameterSupport)this.getStatement();
        final NamedParameter param = paramStmt.getNamedParameters().getParameter(i - 1);
        param.setParameter(obj);
    }

    public void setNull(final int i, final int i1) throws HBqlException {
        this.setParameter(i, i1);
    }

    public void setBoolean(final int i, final boolean b) throws HBqlException {
        this.setParameter(i, b);
    }

    public void setByte(final int i, final byte b) throws HBqlException {
        this.setParameter(i, b);
    }

    public void setShort(final int i, final short i2) throws HBqlException {
        this.setParameter(i, i2);
    }

    public void setInt(final int i, final int i1) throws HBqlException {
        this.setParameter(i, i1);
    }

    public void setLong(final int i, final long l) throws HBqlException {
        this.setParameter(i, l);
    }

    public void setFloat(final int i, final float v) throws HBqlException {
        this.setParameter(i, v);
    }

    public void setDouble(final int i, final double v) throws HBqlException {
        this.setParameter(i, v);
    }

    public void setBigDecimal(final int i, final BigDecimal bigDecimal) throws HBqlException {
        this.setParameter(i, bigDecimal);
    }

    public void setString(final int i, final String s) throws HBqlException {
        this.setParameter(i, s);
    }

    public void setBytes(final int i, final byte[] bytes) throws HBqlException {
        this.setParameter(i, bytes);
    }

    public void setDate(final int i, final Date date) throws HBqlException {
        this.setParameter(i, date);
    }

    public void setTime(final int i, final Time time) throws HBqlException {
        this.setParameter(i, time);
    }

    public void setTimestamp(final int i, final Timestamp timestamp) throws HBqlException {
        this.setParameter(i, timestamp);
    }

    public void setAsciiStream(final int i, final InputStream inputStream, final int i1) throws HBqlException {
    }

    public void setUnicodeStream(final int i, final InputStream inputStream, final int i1) throws HBqlException {
    }

    public void setBinaryStream(final int i, final InputStream inputStream, final int i1) throws HBqlException {
    }

    public void clearParameters() throws HBqlException {

    }

    public void setObject(final int i, final Object o, final int i1) throws HBqlException {

    }

    public void setObject(final int i, final Object o) throws HBqlException {
        this.setParameter(i, o);
    }

    public void addBatch() throws SQLException {

    }

    public void setCharacterStream(final int i, final Reader reader, final int i1) throws SQLException {

    }

    public void setRef(final int i, final Ref ref) throws SQLException {

    }

    public void setBlob(final int i, final Blob blob) throws SQLException {

    }

    public void setClob(final int i, final Clob clob) throws SQLException {

    }

    public void setArray(final int i, final Array array) throws SQLException {

    }

    public void setDate(final int i, final Date date, final Calendar calendar) throws SQLException {

    }

    public void setTime(final int i, final Time time, final Calendar calendar) throws SQLException {

    }

    public void setTimestamp(final int i, final Timestamp timestamp, final Calendar calendar) throws SQLException {

    }

    public void setNull(final int i, final int i1, final String s) throws SQLException {

    }

    public void setURL(final int i, final URL url) throws SQLException {

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
