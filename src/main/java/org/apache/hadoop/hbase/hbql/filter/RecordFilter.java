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

package org.apache.hadoop.hbase.hbql.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HRecordImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;
import org.apache.hadoop.hbase.hbql.mapping.MappingContext;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Arrays;
import java.util.List;

public class RecordFilter extends InstrumentedFilter {

    private static final Log LOG = LogFactory.getLog(RecordFilter.class);

    private boolean        verbose        = false;
    private ExpressionTree expressionTree = null;

    public HRecordImpl record = new HRecordImpl((MappingContext) null);

    public RecordFilter() {
    }

    private RecordFilter(final ExpressionTree expressionTree) {
        this.expressionTree = expressionTree;
        this.getHRecord().setMappingContext(this.getExpressionTree().getMappingContext());
    }

    public static RecordFilter newRecordFilter(final ExpressionTree expressionTree) {
        return (expressionTree == null) ? null : new RecordFilter(expressionTree);
    }

    public boolean hasFilterRow() {
        return true;
    }

    private HRecordImpl getHRecord() {
        return this.record;
    }

    private TableMapping getMapping() {
        return this.getExpressionTree().getTableMapping();
    }

    private ExpressionTree getExpressionTree() {
        return this.expressionTree;
    }

    private boolean hasValidExpressionTree() {
        return this.getExpressionTree() != null;
    }

    public void setVerbose(final boolean verbose) {
        this.verbose = verbose;
    }

    public boolean getVerbose() {
        return this.verbose;
    }

    public void reset() {
        if (this.getVerbose())
            LOG.debug("In reset()");
        this.getHRecord().clearValues();
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        // LOG.debug("In filterRowKey()");
        //final String rowKey = new String(buffer, offset, length);
        final byte[] rowKey = Arrays.copyOfRange(buffer, offset, length);
        try {
            this.getMapping().getKeyAttrib().setCurrentValue(record, 0, rowKey);
        }
        catch (HBqlException e) {
            Utils.logException(LOG, e);
        }
        return false;
    }

    public boolean filterAllRemaining() {
        return false;
    }

    public void filterRow(List<KeyValue> keyValues) {
        // TODO This is new
    }

    public ReturnCode filterKeyValue(KeyValue v) {

        if (this.getVerbose())
            LOG.debug("In filterKeyValue()");

        if (this.hasValidExpressionTree()) {

            try {
                final String familyName = Bytes.toString(v.getFamily());
                final String columnName = Bytes.toString(v.getQualifier());
                final TableMapping tableMapping = this.getMapping();
                final ColumnAttrib attrib = tableMapping.getAttribFromFamilyQualifiedName(familyName, columnName);

                // Do not bother setting value if it is not used in expression
                if (this.getExpressionTree().getAttribsUsedInExpr().contains(attrib)) {
                    if (this.getVerbose())
                        LOG.debug("In filterKeyValue() setting value for: " + familyName + ":" + columnName);
                    final Object val = attrib.getValueFromBytes(null, v.getValue());
                    this.getHRecord().setCurrentValue(familyName, columnName, v.getTimestamp(), val);
                    this.getHRecord().setVersionValue(familyName, columnName, v.getTimestamp(), val, true);
                }
            }
            catch (Exception e) {
                Utils.logException(LOG, e);
                LOG.debug("Exception in filterKeyValue(): " + e.getClass().getName() + " - " + e.getMessage());
            }
        }

        return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {

        if (this.getVerbose())
            LOG.debug("In filterRow()");

        boolean filterRow;
        if (!this.hasValidExpressionTree()) {
            if (this.getVerbose())
                LOG.debug("In filterRow() had invalid hasValidExpressionTree(): ");
            filterRow = false;
        } else {
            try {
                filterRow = !this.getExpressionTree().evaluate(null, this.getHRecord());
                if (this.getVerbose())
                    LOG.debug("In filterRow() filtering record: " + filterRow);
            }
            catch (ResultMissingColumnException e) {
                if (this.getVerbose())
                    LOG.debug("In filterRow() had ResultMissingColumnException exception: " + e.getMessage());
                filterRow = true;
            }
            catch (NullColumnValueException e) {
                if (this.getVerbose())
                    LOG.debug("In filterRow() had NullColumnValueException exception: " + e.getMessage());
                filterRow = true;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                Utils.logException(LOG, e);
                LOG.debug("In filterRow() had HBqlException: " + e.getMessage());
                filterRow = true;
            }
        }

        //LOG.debug("In filterRow() returning: " + filterRow);
        return filterRow;
    }

    public void write(DataOutput out) throws IOException {
        try {
            out.writeBoolean(this.getVerbose());
            final byte[] b = IO.getSerialization().getObjectAsBytes(this.getExpressionTree());
            Bytes.writeByteArray(out, b);
        }
        catch (HBqlException e) {
            e.printStackTrace();
            Utils.logException(LOG, e);
            throw new IOException(e.getCause());
        }
    }

    public void readFields(DataInput in) throws IOException {

        try {
            this.verbose = in.readBoolean();
            final byte[] b = Bytes.readByteArray(in);
            this.expressionTree = (ExpressionTree) IO.getSerialization().getScalarFromBytes(FieldType.ObjectType, b);

            this.getHRecord().setMappingContext(this.getExpressionTree().getMappingContext());
            this.getMapping().resetDefaultValues();
        }
        catch (HBqlException e) {
            e.printStackTrace();
            Utils.logException(LOG, e);
            throw new IOException(e.getCause());
        }
    }

    public static void testFilter(final RecordFilter origFilter) throws HBqlException, IOException {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        origFilter.write(oos);
        oos.flush();
        oos.close();
        final byte[] b = baos.toByteArray();

        final ByteArrayInputStream bais = new ByteArrayInputStream(b);
        final ObjectInputStream ois = new ObjectInputStream(bais);

        RecordFilter filter = new RecordFilter();
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
            filter.getHRecord().setCurrentValue(family, column, 100, val);
            filter.getHRecord().setVersionValue(family, column, 100, val, true);
        }

        boolean v = filter.filterRow();
    }
}
