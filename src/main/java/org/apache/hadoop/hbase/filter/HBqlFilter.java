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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.query.expr.ExprTree;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.DefinedSchema;
import org.apache.hadoop.hbase.hbql.query.schema.FieldType;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
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

    private ExprTree exprTree;
    private long scanLimit = -1;
    private long recordCount = 0;
    public transient HRecord record = new HRecord((HBaseSchema)null);

    public HBqlFilter(final ExprTree exprTree, final long scanLimit) {
        this.exprTree = exprTree;
        this.scanLimit = scanLimit;
        this.recordCount = 0;
        this.getRecord().setSchema(this.getSchema());
    }

    public HBqlFilter() {
    }

    private HRecord getRecord() {
        return this.record;
    }

    private DefinedSchema getSchema() {
        return (DefinedSchema)this.getExprTree().getSchema();
    }

    private ExprTree getExprTree() {
        return this.exprTree;
    }

    private long getScanLimit() {
        return this.scanLimit;
    }

    private long getRecordCount() {
        return this.recordCount;
    }

    private void incrementRecordCount() {
        this.recordCount++;
    }

    private boolean hasValidExprTree() {
        return this.getExprTree() != null && getExprTree().isValid();
    }

    public void reset() {
        LOG.info("In reset()");
        this.getRecord().clear();
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        LOG.info("In filterRowKey()");
        return false;
    }

    public boolean filterAllRemaining() {
        final boolean retval = this.getScanLimit() > 0 && this.getRecordCount() >= this.getScanLimit();
        LOG.info("In filterAllRemaining() " + this.getScanLimit() + " - " + this.getRecordCount() + " - " + retval);
        return retval;
    }

    public ReturnCode filterKeyValue(KeyValue v) {

        LOG.info("In filterKeyValue()");

        if (this.hasValidExprTree()) {

            final String familyName = Bytes.toString(v.getFamily());
            final String columnName = Bytes.toString(v.getQualifier());
            final DefinedSchema schema = this.getSchema();
            final ColumnAttrib attrib = schema.getAttribFromFamilyQualifiedName(familyName, columnName);

            // Do not bother setting value if it is not used in expression
            if (this.getExprTree().getAttribsUsedInExpr().contains(attrib)) {
                try {
                    LOG.info("In in filterKeyValue() setting value for: " + familyName + ":" + columnName);
                    final Object val = attrib.getValueFromBytes(null, v.getValue());
                    this.getRecord().setCurrentValue(familyName, columnName, v.getTimestamp(), val);
                    this.getRecord().setVersionedValue(familyName, columnName, v.getTimestamp(), val);
                }
                catch (Exception e) {
                    HUtil.logException(LOG, e);
                    LOG.info("Had exception in filterKeyValue(): " + e.getClass().getName() + " - " + e.getMessage());
                }
            }
        }

        return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {

        LOG.info("In filterRow()");

        if (!this.hasValidExprTree()) {
            this.incrementRecordCount();
            return false;
        }
        else {
            try {
                final boolean filterRecord = !this.getExprTree().evaluate(this.getRecord());
                if (!filterRecord)
                    this.incrementRecordCount();
                return filterRecord;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                HUtil.logException(LOG, e);
                LOG.info("In filterRow() had exception: " + e.getMessage());
                return true;
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        try {
            Bytes.writeByteArray(out, HUtil.ser.getScalarAsBytes(this.getExprTree()));
            Bytes.writeByteArray(out, HUtil.ser.getScalarAsBytes(FieldType.LongType, this.getScanLimit()));
        }
        catch (HBqlException e) {
            e.printStackTrace();
            HUtil.logException(LOG, e);
            throw new IOException("HPersistException: " + e.getCause());
        }
    }

    public void readFields(DataInput in) throws IOException {

        LOG.info("In readFields()");

        try {
            this.exprTree = (ExprTree)HUtil.ser.getScalarFromBytes(FieldType.ObjectType, Bytes.readByteArray(in));
            this.scanLimit = (Long)HUtil.ser.getScalarFromBytes(FieldType.LongType, Bytes.readByteArray(in));
            this.getRecord().setSchema(this.getSchema());
            this.recordCount = 0;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            LOG.info("In read(): " + e.getCause());
            throw new IOException("HPersistException: " + e.getCause());
        }
    }

    public static void testFilter(final HBqlFilter origFilter) throws IOException, HBqlException {

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

        final String family = "family1";
        final String column = "author";
        final String[] vals = {"An author value-81252702162528282000",
                               "An author value-812527021593753270002009",
                               "An author value-81252702156610125000",
                               "An author value-812527021520532270002009",
                               "An author value-81252702147337884000"
        };

        for (String val : vals) {
            filter.getRecord().setCurrentValue(family, column, 100, val);
            filter.getRecord().setVersionedValue(family, column, 100, val);
        }

        boolean v = filter.filterRow();
        return;

    }

}