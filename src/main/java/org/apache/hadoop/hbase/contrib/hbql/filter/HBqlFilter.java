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

package org.apache.hadoop.hbase.contrib.hbql.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.HBqlException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.contrib.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.contrib.hbql.io.IO;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.schema.DefinedSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.FieldType;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

public class HBqlFilter implements Filter {

    private static final Log LOG = LogFactory.getLog(HBqlFilter.class.getName());

    private ExpressionTree expressionTree;
    private long scanLimit = -1;
    private long recordCount = 0;
    public transient RecordImpl record = new RecordImpl((HBaseSchema)null);

    public HBqlFilter(final ExpressionTree expressionTree, final long scanLimit) {
        this.expressionTree = expressionTree;
        this.scanLimit = scanLimit;
        this.recordCount = 0;
        this.getRecord().setSchema(this.getSchema());
    }

    public HBqlFilter() {
    }

    private RecordImpl getRecord() {
        return this.record;
    }

    private DefinedSchema getSchema() {
        return (DefinedSchema)this.getExpressionTree().getSchema();
    }

    private ExpressionTree getExpressionTree() {
        return this.expressionTree;
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

    private boolean hasValidExpressionTree() {
        return this.getExpressionTree() != null;
    }

    public void reset() {
        LOG.info("In reset()");
        this.getRecord().clearValues();
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

        if (this.hasValidExpressionTree()) {

            final String familyName = Bytes.toString(v.getFamily());
            final String columnName = Bytes.toString(v.getQualifier());
            final DefinedSchema schema = this.getSchema();
            final ColumnAttrib attrib = schema.getAttribFromFamilyQualifiedName(familyName, columnName);

            // Do not bother setting value if it is not used in expression
            if (this.getExpressionTree().getAttribsUsedInExpr().contains(attrib)) {
                try {
                    LOG.info("In in filterKeyValue() setting value for: " + familyName + ":" + columnName);
                    final Object val = attrib.getValueFromBytes(null, v.getValue());
                    this.getRecord().setCurrentValue(familyName, columnName, v.getTimestamp(), val);
                    this.getRecord().setVersionValue(familyName, columnName, v.getTimestamp(), val, true);
                }
                catch (Exception e) {
                    logException(LOG, e);
                    LOG.info("Had exception in filterKeyValue(): " + e.getClass().getName() + " - " + e.getMessage());
                }
            }
        }

        return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {

        LOG.info("In filterRow()");

        if (!this.hasValidExpressionTree()) {
            this.incrementRecordCount();
            return false;
        }
        else {
            try {
                final boolean filterRecord = !this.getExpressionTree().evaluate(this.getRecord());
                if (!filterRecord)
                    this.incrementRecordCount();
                return filterRecord;
            }
            catch (ResultMissingColumnException e) {
                return true;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                logException(LOG, e);
                LOG.info("In filterRow() had exception: " + e.getMessage());
                return true;
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        try {
            Bytes.writeByteArray(out, IO.getSerialization().getScalarAsBytes(this.getExpressionTree()));
            Bytes.writeByteArray(out, IO.getSerialization().getScalarAsBytes(FieldType.LongType, this.getScanLimit()));
        }
        catch (HBqlException e) {
            e.printStackTrace();
            logException(LOG, e);
            throw new IOException("HPersistException: " + e.getCause());
        }
    }

    public void readFields(DataInput in) throws IOException {

        LOG.info("In readFields()");

        try {
            this.expressionTree = (ExpressionTree)IO.getSerialization().getScalarFromBytes(FieldType.ObjectType,
                                                                                           Bytes.readByteArray(in));
            this.scanLimit = (Long)IO.getSerialization().getScalarFromBytes(FieldType.LongType,
                                                                            Bytes.readByteArray(in));
            this.getRecord().setSchema(this.getSchema());

            this.getSchema().resetDefaultValues();

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
            filter.getRecord().setVersionValue(family, column, 100, val, true);
        }

        boolean v = filter.filterRow();
    }

    public static void logException(final Log log, final Exception e) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final PrintWriter oos = new PrintWriter(baos);

        e.printStackTrace(oos);
        oos.flush();
        oos.close();

        log.info(baos.toString());
    }
}
