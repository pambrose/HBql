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

import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.client.Results;
import org.apache.hadoop.hbase.hbql.impl.RecordImpl;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

public class ResultSetImpl implements ResultSet {

    final Results<HRecord> results;
    private Iterator<HRecord> resultsIterator;
    private RecordImpl currentRecord = null;

    public ResultSetImpl(final Results<HRecord> results) {
        this.results = results;
        this.resultsIterator = results.iterator();
    }

    private Results<HRecord> getResults() {
        return this.results;
    }

    private Iterator<HRecord> getResultsIterator() {
        return this.resultsIterator;
    }

    private RecordImpl getCurrentRecord() {
        return this.currentRecord;
    }

    private void setCurrentRecord(final RecordImpl currentRecord) {
        this.currentRecord = currentRecord;
    }

    public <T> T unwrap(final Class<T> tClass) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(final Class<?> aClass) throws SQLException {
        return false;
    }

    public boolean next() throws SQLException {
        if (this.getResultsIterator().hasNext()) {
            this.setCurrentRecord((RecordImpl)this.getResultsIterator().next());
            return true;
        }
        else {
            return false;
        }
    }

    public void close() throws SQLException {

    }

    public boolean wasNull() throws SQLException {
        return false;
    }

    public String getString(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getString(name);
    }

    public boolean getBoolean(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getBoolean(name);
    }

    public byte getByte(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getByte(name);
    }

    public short getShort(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getShort(name);
    }

    public int getInt(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getInt(name);
    }

    public long getLong(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getInt(name);
    }

    public float getFloat(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getInt(name);
    }

    public double getDouble(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getInt(name);
    }

    public BigDecimal getBigDecimal(final int i, final int i1) throws SQLException {
        return null;
    }

    public byte[] getBytes(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getBytes(name);
    }

    public Date getDate(final int i) throws SQLException {
        final String name = this.getCurrentRecord().getAttribName(i);
        return this.getDate(name);
    }

    public Time getTime(final int i) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(final int i) throws SQLException {
        return null;
    }

    public InputStream getAsciiStream(final int i) throws SQLException {
        return null;
    }

    public InputStream getUnicodeStream(final int i) throws SQLException {
        return null;
    }

    public InputStream getBinaryStream(final int i) throws SQLException {
        return null;
    }

    public String getString(final String s) throws SQLException {
        return ((String)this.getCurrentRecord().getCurrentValue(s));
    }

    public boolean getBoolean(final String s) throws SQLException {
        return ((Boolean)this.getCurrentRecord().getCurrentValue(s));
    }

    public byte getByte(final String s) throws SQLException {
        return ((Byte)this.getCurrentRecord().getCurrentValue(s));
    }

    public short getShort(final String s) throws SQLException {
        return ((Number)this.getCurrentRecord().getCurrentValue(s)).shortValue();
    }

    public int getInt(final String s) throws SQLException {
        return ((Number)this.getCurrentRecord().getCurrentValue(s)).intValue();
    }

    public long getLong(final String s) throws SQLException {
        return ((Number)this.getCurrentRecord().getCurrentValue(s)).longValue();
    }

    public float getFloat(final String s) throws SQLException {
        return ((Number)this.getCurrentRecord().getCurrentValue(s)).floatValue();
    }

    public double getDouble(final String s) throws SQLException {
        return ((Number)this.getCurrentRecord().getCurrentValue(s)).doubleValue();
    }

    public BigDecimal getBigDecimal(final String s, final int i) throws SQLException {
        return null;
    }

    public byte[] getBytes(final String s) throws SQLException {
        return ((byte[])this.getCurrentRecord().getCurrentValue(s));
    }

    public Date getDate(final String s) throws SQLException {
        return ((Date)this.getCurrentRecord().getCurrentValue(s));
    }

    public Time getTime(final String s) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(final String s) throws SQLException {
        return null;
    }

    public InputStream getAsciiStream(final String s) throws SQLException {
        return null;
    }

    public InputStream getUnicodeStream(final String s) throws SQLException {
        return null;
    }

    public InputStream getBinaryStream(final String s) throws SQLException {
        return null;
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public String getCursorName() throws SQLException {
        return null;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    public Object getObject(final int i) throws SQLException {
        return null;
    }

    public Object getObject(final String s) throws SQLException {
        return this.getCurrentRecord().getCurrentValue(s);
    }

    public int findColumn(final String s) throws SQLException {
        return 0;
    }

    public Reader getCharacterStream(final int i) throws SQLException {
        return null;
    }

    public Reader getCharacterStream(final String s) throws SQLException {
        return null;
    }

    public BigDecimal getBigDecimal(final int i) throws SQLException {
        return null;
    }

    public BigDecimal getBigDecimal(final String s) throws SQLException {
        return null;
    }

    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    public boolean isAfterLast() throws SQLException {
        return false;
    }

    public boolean isFirst() throws SQLException {
        return false;
    }

    public boolean isLast() throws SQLException {
        return false;
    }

    public void beforeFirst() throws SQLException {

    }

    public void afterLast() throws SQLException {

    }

    public boolean first() throws SQLException {
        return false;
    }

    public boolean last() throws SQLException {
        return false;
    }

    public int getRow() throws SQLException {
        return 0;
    }

    public boolean absolute(final int i) throws SQLException {
        return false;
    }

    public boolean relative(final int i) throws SQLException {
        return false;
    }

    public boolean previous() throws SQLException {
        return false;
    }

    public void setFetchDirection(final int i) throws SQLException {

    }

    public int getFetchDirection() throws SQLException {
        return 0;
    }

    public void setFetchSize(final int i) throws SQLException {

    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getType() throws SQLException {
        return 0;
    }

    public int getConcurrency() throws SQLException {
        return 0;
    }

    public boolean rowUpdated() throws SQLException {
        return false;
    }

    public boolean rowInserted() throws SQLException {
        return false;
    }

    public boolean rowDeleted() throws SQLException {
        return false;
    }

    public void updateNull(final int i) throws SQLException {

    }

    public void updateBoolean(final int i, final boolean b) throws SQLException {

    }

    public void updateByte(final int i, final byte b) throws SQLException {

    }

    public void updateShort(final int i, final short i1) throws SQLException {

    }

    public void updateInt(final int i, final int i1) throws SQLException {

    }

    public void updateLong(final int i, final long l) throws SQLException {

    }

    public void updateFloat(final int i, final float v) throws SQLException {

    }

    public void updateDouble(final int i, final double v) throws SQLException {

    }

    public void updateBigDecimal(final int i, final BigDecimal bigDecimal) throws SQLException {

    }

    public void updateString(final int i, final String s) throws SQLException {

    }

    public void updateBytes(final int i, final byte[] bytes) throws SQLException {

    }

    public void updateDate(final int i, final Date date) throws SQLException {

    }

    public void updateTime(final int i, final Time time) throws SQLException {

    }

    public void updateTimestamp(final int i, final Timestamp timestamp) throws SQLException {

    }

    public void updateAsciiStream(final int i, final InputStream inputStream, final int i1) throws SQLException {

    }

    public void updateBinaryStream(final int i, final InputStream inputStream, final int i1) throws SQLException {

    }

    public void updateCharacterStream(final int i, final Reader reader, final int i1) throws SQLException {

    }

    public void updateObject(final int i, final Object o, final int i1) throws SQLException {

    }

    public void updateObject(final int i, final Object o) throws SQLException {

    }

    public void updateNull(final String s) throws SQLException {

    }

    public void updateBoolean(final String s, final boolean b) throws SQLException {

    }

    public void updateByte(final String s, final byte b) throws SQLException {

    }

    public void updateShort(final String s, final short i) throws SQLException {

    }

    public void updateInt(final String s, final int i) throws SQLException {

    }

    public void updateLong(final String s, final long l) throws SQLException {

    }

    public void updateFloat(final String s, final float v) throws SQLException {

    }

    public void updateDouble(final String s, final double v) throws SQLException {

    }

    public void updateBigDecimal(final String s, final BigDecimal bigDecimal) throws SQLException {

    }

    public void updateString(final String s, final String s1) throws SQLException {

    }

    public void updateBytes(final String s, final byte[] bytes) throws SQLException {

    }

    public void updateDate(final String s, final Date date) throws SQLException {

    }

    public void updateTime(final String s, final Time time) throws SQLException {

    }

    public void updateTimestamp(final String s, final Timestamp timestamp) throws SQLException {

    }

    public void updateAsciiStream(final String s, final InputStream inputStream, final int i) throws SQLException {

    }

    public void updateBinaryStream(final String s, final InputStream inputStream, final int i) throws SQLException {

    }

    public void updateCharacterStream(final String s, final Reader reader, final int i) throws SQLException {

    }

    public void updateObject(final String s, final Object o, final int i) throws SQLException {

    }

    public void updateObject(final String s, final Object o) throws SQLException {

    }

    public void insertRow() throws SQLException {

    }

    public void updateRow() throws SQLException {

    }

    public void deleteRow() throws SQLException {

    }

    public void refreshRow() throws SQLException {

    }

    public void cancelRowUpdates() throws SQLException {

    }

    public void moveToInsertRow() throws SQLException {

    }

    public void moveToCurrentRow() throws SQLException {

    }

    public Statement getStatement() throws SQLException {
        return null;
    }

    public Object getObject(final int i, final Map<String, Class<?>> stringClassMap) throws SQLException {
        return null;
    }

    public Ref getRef(final int i) throws SQLException {
        return null;
    }

    public Blob getBlob(final int i) throws SQLException {
        return null;
    }

    public Clob getClob(final int i) throws SQLException {
        return null;
    }

    public Array getArray(final int i) throws SQLException {
        return null;
    }

    public Object getObject(final String s, final Map<String, Class<?>> stringClassMap) throws SQLException {
        return null;
    }

    public Ref getRef(final String s) throws SQLException {
        return null;
    }

    public Blob getBlob(final String s) throws SQLException {
        return null;
    }

    public Clob getClob(final String s) throws SQLException {
        return null;
    }

    public Array getArray(final String s) throws SQLException {
        return null;
    }

    public Date getDate(final int i, final Calendar calendar) throws SQLException {
        return null;
    }

    public Date getDate(final String s, final Calendar calendar) throws SQLException {
        return null;
    }

    public Time getTime(final int i, final Calendar calendar) throws SQLException {
        return null;
    }

    public Time getTime(final String s, final Calendar calendar) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(final int i, final Calendar calendar) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(final String s, final Calendar calendar) throws SQLException {
        return null;
    }

    public URL getURL(final int i) throws SQLException {
        return null;
    }

    public URL getURL(final String s) throws SQLException {
        return null;
    }

    public void updateRef(final int i, final Ref ref) throws SQLException {

    }

    public void updateRef(final String s, final Ref ref) throws SQLException {

    }

    public void updateBlob(final int i, final Blob blob) throws SQLException {

    }

    public void updateBlob(final String s, final Blob blob) throws SQLException {

    }

    public void updateClob(final int i, final Clob clob) throws SQLException {

    }

    public void updateClob(final String s, final Clob clob) throws SQLException {

    }

    public void updateArray(final int i, final Array array) throws SQLException {

    }

    public void updateArray(final String s, final Array array) throws SQLException {

    }

    public RowId getRowId(final int i) throws SQLException {
        return new RowIdImpl(this.getString(i));
    }

    public RowId getRowId(final String s) throws SQLException {
        return new RowIdImpl(this.getString(s));
    }

    public void updateRowId(final int i, final RowId rowId) throws SQLException {

    }

    public void updateRowId(final String s, final RowId rowId) throws SQLException {

    }

    public int getHoldability() throws SQLException {
        return 0;
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void updateNString(final int i, final String s) throws SQLException {

    }

    public void updateNString(final String s, final String s1) throws SQLException {

    }

    public void updateNClob(final int i, final NClob nClob) throws SQLException {

    }

    public void updateNClob(final String s, final NClob nClob) throws SQLException {

    }

    public NClob getNClob(final int i) throws SQLException {
        return null;
    }

    public NClob getNClob(final String s) throws SQLException {
        return null;
    }

    public SQLXML getSQLXML(final int i) throws SQLException {
        return null;
    }

    public SQLXML getSQLXML(final String s) throws SQLException {
        return null;
    }

    public void updateSQLXML(final int i, final SQLXML sqlxml) throws SQLException {

    }

    public void updateSQLXML(final String s, final SQLXML sqlxml) throws SQLException {

    }

    public String getNString(final int i) throws SQLException {
        return null;
    }

    public String getNString(final String s) throws SQLException {
        return null;
    }

    public Reader getNCharacterStream(final int i) throws SQLException {
        return null;
    }

    public Reader getNCharacterStream(final String s) throws SQLException {
        return null;
    }

    public void updateNCharacterStream(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void updateNCharacterStream(final String s, final Reader reader, final long l) throws SQLException {

    }

    public void updateAsciiStream(final int i, final InputStream inputStream, final long l) throws SQLException {

    }

    public void updateBinaryStream(final int i, final InputStream inputStream, final long l) throws SQLException {

    }

    public void updateCharacterStream(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void updateAsciiStream(final String s, final InputStream inputStream, final long l) throws SQLException {

    }

    public void updateBinaryStream(final String s, final InputStream inputStream, final long l) throws SQLException {

    }

    public void updateCharacterStream(final String s, final Reader reader, final long l) throws SQLException {

    }

    public void updateBlob(final int i, final InputStream inputStream, final long l) throws SQLException {

    }

    public void updateBlob(final String s, final InputStream inputStream, final long l) throws SQLException {

    }

    public void updateClob(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void updateClob(final String s, final Reader reader, final long l) throws SQLException {

    }

    public void updateNClob(final int i, final Reader reader, final long l) throws SQLException {

    }

    public void updateNClob(final String s, final Reader reader, final long l) throws SQLException {

    }

    public void updateNCharacterStream(final int i, final Reader reader) throws SQLException {

    }

    public void updateNCharacterStream(final String s, final Reader reader) throws SQLException {

    }

    public void updateAsciiStream(final int i, final InputStream inputStream) throws SQLException {

    }

    public void updateBinaryStream(final int i, final InputStream inputStream) throws SQLException {

    }

    public void updateCharacterStream(final int i, final Reader reader) throws SQLException {

    }

    public void updateAsciiStream(final String s, final InputStream inputStream) throws SQLException {

    }

    public void updateBinaryStream(final String s, final InputStream inputStream) throws SQLException {

    }

    public void updateCharacterStream(final String s, final Reader reader) throws SQLException {

    }

    public void updateBlob(final int i, final InputStream inputStream) throws SQLException {

    }

    public void updateBlob(final String s, final InputStream inputStream) throws SQLException {

    }

    public void updateClob(final int i, final Reader reader) throws SQLException {

    }

    public void updateClob(final String s, final Reader reader) throws SQLException {

    }

    public void updateNClob(final int i, final Reader reader) throws SQLException {

    }

    public void updateNClob(final String s, final Reader reader) throws SQLException {

    }
}
