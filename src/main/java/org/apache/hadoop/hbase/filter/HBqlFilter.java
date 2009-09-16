/*
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.filter;

import com.imap4j.hbase.hbase.HPersistException;
import com.imap4j.hbase.hbase.HRecord;
import com.imap4j.hbase.hbql.expr.ExprTree;
import com.imap4j.hbase.hbql.schema.ColumnAttrib;
import com.imap4j.hbase.hbql.schema.DefinedSchema;
import com.imap4j.hbase.hbql.schema.FieldType;
import com.imap4j.hbase.hbql.schema.HUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class HBqlFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(HBqlFilter.class.getName());

    private DefinedSchema schema;
    private ExprTree filterExpr;
    public transient HRecord record = new HRecord(null);

    public HBqlFilter(final DefinedSchema schema, final ExprTree filterExpr) {
        this.schema = schema;
        this.filterExpr = filterExpr;
        record.setSchema(this.schema);
    }

    public HBqlFilter() {
    }

    public void reset() {
        LOG.info("PRA called reset()");
        this.record.clear();
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        return false;
    }

    public boolean filterAllRemaining() {
        return false;
    }

    private HRecord getRecord() {
        return this.record;
    }

    public ReturnCode filterKeyValue(KeyValue v) {
        String qualColName = new String(v.getColumn());
        try {
            final ColumnAttrib attrib = schema.getColumnAttribByFamilyQualifiedColumnName(qualColName);
            final Object val = attrib.getValueFromBytes(HUtil.ser, null, v.getValue());
            LOG.info("PRA setting value for: " + qualColName + " - " + val);

            this.getRecord().setCurrentValueByFamilyQualifiedName(qualColName, v.getTimestamp(), val);
            this.getRecord().setVersionedValueByFamilyQualifiedName(qualColName, v.getTimestamp(), val);
        }
        catch (Exception e) {
            HUtil.logException(LOG, e);
            LOG.info("PRA3 had exception: " + e.getClass().getName() + " - " + e.getMessage());
        }

        return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {

        if (this.filterExpr == null)
            return false;

        LOG.info("PRA evaluating #2");

        try {
            final boolean val = !this.filterExpr.evaluate(this.record);
            LOG.info("PRA returning " + val);
            return val;
        }
        catch (HPersistException e) {
            e.printStackTrace();
            HUtil.logException(LOG, e);
            LOG.info("PRA4 had exception: " + e.getMessage());
            return true;
        }
    }

    public void write(DataOutput out) throws IOException {
        try {
            Bytes.writeByteArray(out, HUtil.ser.getObjectAsBytes(this.schema));
            Bytes.writeByteArray(out, HUtil.ser.getObjectAsBytes(this.filterExpr));
        }
        catch (HPersistException e) {
            e.printStackTrace();
            HUtil.logException(LOG, e);
            LOG.info("HBqlFilter problem: " + e.getCause());
            throw new IOException("HBqlFilter problem: " + e.getCause());
        }
    }

    public void readFields(DataInput in) throws IOException {
        try {
            this.schema = (DefinedSchema)HUtil.ser.getObjectFromBytes(FieldType.ObjectType, Bytes.readByteArray(in));
            this.filterExpr = (ExprTree)HUtil.ser.getObjectFromBytes(FieldType.ObjectType, Bytes.readByteArray(in));
            record.setSchema(this.schema);
        }
        catch (HPersistException e) {
            e.printStackTrace();
            LOG.info("HBqlFilter problem: " + e.getCause());
            throw new IOException("HBqlFilter problem: " + e.getCause());
        }
    }

    public static void testFilter(final HBqlFilter origFilter) throws IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        origFilter.write(oos);
        oos.flush();
        oos.close();
        final byte[] b = baos.toByteArray();

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        HBqlFilter filter = new HBqlFilter();
        filter.readFields(ois);

        filter.reset();

        final String colname = "family1:author";
        final String[] vals = {"An author value-81252702162528282000",
                               "An author value-812527021593753270002009",
                               "An author value-81252702156610125000",
                               "An author value-812527021520532270002009",
                               "An author value-81252702147337884000"
        };

        for (String val : vals) {
            filter.record.setCurrentValueByFamilyQualifiedName(colname, 100, val);
            filter.record.setVersionedValueByFamilyQualifiedName(colname, 100, val);
        }

        boolean v = filter.filterRow();
        return;

    }

}