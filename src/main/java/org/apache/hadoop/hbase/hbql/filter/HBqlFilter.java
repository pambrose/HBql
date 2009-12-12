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

package org.apache.hadoop.hbase.hbql.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HRecordImpl;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
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
    public transient HRecordImpl record = new HRecordImpl((StatementContext)null);

    public HBqlFilter(final ExpressionTree expressionTree) {
        this.expressionTree = expressionTree;
        this.getHRecord().setStatementContext(this.getExpressionTree().getStatementContext());
    }

    public HBqlFilter() {
    }

    public static HBqlFilter newHBqlFilter(final StatementContext statementContext,
                                           final ExpressionTree origExpressionTree) throws HBqlException {

        if (origExpressionTree == null)
            return null;

        origExpressionTree.setStatementContext(statementContext);
        return new HBqlFilter(origExpressionTree);
    }

    private HRecordImpl getHRecord() {
        return this.record;
    }

    private TableMapping getMapping() throws HBqlException {
        return this.getExpressionTree().getTableMapping();
    }

    private ExpressionTree getExpressionTree() {
        return this.expressionTree;
    }

    private boolean hasValidExpressionTree() {
        return this.getExpressionTree() != null;
    }

    public void reset() {
        LOG.info("In reset()");
        this.getHRecord().clearValues();
    }

    public boolean filterRowKey(byte[] buffer, int offset, int length) {
        LOG.info("In filterRowKey()");
        return false;
    }

    public boolean filterAllRemaining() {
        return false;
    }

    public ReturnCode filterKeyValue(KeyValue v) {

        LOG.info("In filterKeyValue()");

        if (this.hasValidExpressionTree()) {

            try {
                final String familyName = Bytes.toString(v.getFamily());
                final String columnName = Bytes.toString(v.getQualifier());
                final TableMapping tableMapping = this.getMapping();
                final ColumnAttrib attrib = tableMapping.getAttribFromFamilyQualifiedName(familyName, columnName);

                // Do not bother setting value if it is not used in expression
                if (this.getExpressionTree().getAttribsUsedInExprs().contains(attrib)) {
                    LOG.info("In in filterKeyValue() setting value for: " + familyName + ":" + columnName);
                    final Object val = attrib.getValueFromBytes(null, v.getValue());
                    this.getHRecord().setCurrentValue(familyName, columnName, v.getTimestamp(), val);
                    this.getHRecord().setVersionValue(familyName, columnName, v.getTimestamp(), val, true);
                }
            }
            catch (Exception e) {
                logException(LOG, e);
                LOG.info("Had exception in filterKeyValue(): " + e.getClass().getName() + " - " + e.getMessage());
            }
        }

        return ReturnCode.INCLUDE;
    }

    public boolean filterRow() {

        LOG.info("In filterRow()");

        if (!this.hasValidExpressionTree()) {
            return false;
        }
        else {
            try {
                final boolean filterRecord = !this.getExpressionTree().evaluate(null, this.getHRecord());
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
            this.getHRecord().setStatementContext(this.getExpressionTree().getStatementContext());

            this.getMapping().resetDefaultValues();
        }
        catch (HBqlException e) {
            e.printStackTrace();
            LOG.info("In read(): " + e.getCause());
            throw new IOException("HPersistException: " + e.getCause());
        }
    }

    public static void testFilter(final HBqlFilter origFilter) throws HBqlException, IOException {

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
            filter.getHRecord().setCurrentValue(family, column, 100, val);
            filter.getHRecord().setVersionValue(family, column, 100, val, true);
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
