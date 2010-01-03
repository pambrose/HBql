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

package org.apache.hadoop.hbase.hbql.statement.select;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.HRecord;
import org.apache.hadoop.hbase.hbql.impl.AggregateValue;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.HRecordImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;

public class SelectExpressionContext extends MultipleExpressionContext implements SelectElement {

    private String asName;

    private ColumnAttrib columnAttrib = null;
    private String familyName = null;
    private String columnName = null;
    private byte[] familyNameBytes = null;
    private byte[] columnNameBytes = null;

    private SelectExpressionContext(final GenericValue genericValue, final String asName) {
        super(null, genericValue);
        this.asName = asName;
    }

    public static SelectExpressionContext newExpression(final GenericValue expr, final String as) {
        return new SelectExpressionContext(expr, as);
    }

    public String getAsName() {
        return this.asName;
    }

    public boolean isAnAggregateElement() {
        return this.getGenericValue().isAnAggregateValue();
    }

    private GenericValue getGenericValue() {
        return this.getGenericValue(0);
    }

    public void initAggregateValue(final AggregateValue aggregateValue) throws HBqlException {
        this.getGenericValue().initAggregateValue(aggregateValue);
    }

    public void applyResultToAggregateValue(final AggregateValue aggregateValue,
                                            final Result result) throws HBqlException,
                                                                        ResultMissingColumnException,
                                                                        NullColumnValueException {
        this.getGenericValue().applyResultToAggregateValue(aggregateValue, result);
    }

    public String getElementName() {
        if (this.hasAsName())
            return this.getAsName();
        return this.getColumnAttrib().getFamilyQualifiedName();
    }

    public boolean isAFamilySelect() {
        return false;
    }

    public boolean hasAsName() {
        return Utils.isValidString(this.getAsName());
    }

    public boolean isADelegateColumnReference() {
        return this.getGenericValue() instanceof DelegateColumn;
    }

    public boolean isAConstant() {
        return this.getGenericValue().isAConstant();
    }

    public boolean isDefaultKeyword() {
        return this.getGenericValue().isDefaultKeyword();
    }

    public boolean hasAColumnReference() {
        return this.getGenericValue().hasAColumnReference();
    }

    public boolean isAKeyValue() {
        if (!this.isADelegateColumnReference())
            return false;

        if (this.getColumnAttrib() != null)
            return this.getColumnAttrib().isAKeyAttrib();

        return false;
    }

    public Class<? extends GenericValue> getExpressionType() throws HBqlException {
        return this.getGenericValue().validateTypes(null, false);
    }

    private ColumnAttrib getColumnAttrib() {
        return this.columnAttrib;
    }

    private String getFamilyName() {
        return this.familyName;
    }

    private String getColumnName() {
        return this.columnName;
    }

    private byte[] getFamilyNameBytes() {
        return this.familyNameBytes;
    }

    private byte[] getColumnNameBytes() {
        return this.columnNameBytes;
    }

    public void validate(final StatementContext statementContext, final HConnection connection) throws HBqlException {

        this.setStatementContext(statementContext);

        // TODO this needs to be done for expressions with col refs

        // Look up stuff for simple column references
        if (this.isADelegateColumnReference()) {
            final String name = ((DelegateColumn)this.getGenericValue()).getVariableName();
            this.columnAttrib = this.getResultAccessor().getColumnAttribByName(name);

            if (this.getColumnAttrib() != null) {
                this.familyName = this.getColumnAttrib().getFamilyName();
                this.columnName = this.getColumnAttrib().getColumnName();
            }
            else {
                if (!name.contains(":"))
                    throw new HBqlException("Unknown select value: " + name);
                final String[] strs = name.split(":");
                this.familyName = strs[0];
                this.columnName = strs[1];
                final Collection<String> families = this.getTableMapping().getMappingFamilyNames();
                if (!families.contains(this.getFamilyName()))
                    throw new HBqlException("Unknown family name: " + this.getFamilyName());
            }

            this.familyNameBytes = IO.getSerialization().getStringAsBytes(this.getFamilyName());
            this.columnNameBytes = IO.getSerialization().getStringAsBytes(this.getColumnName());
        }
    }

    public void assignAsNamesForExpressions(final SelectStatement selectStatement) {

        if (!this.isADelegateColumnReference() && !this.hasAsName()) {
            while (true) {
                // Assign a name that is not in use
                final String newAsName = selectStatement.getNextExpressionName();
                if (!selectStatement.hasAsName(newAsName)) {
                    this.asName = newAsName;
                    break;
                }
            }
        }
    }

    private String getSelectName() {
        return this.hasAsName() ? this.getAsName() : this.getFamilyName() + ":" + this.getColumnName();
    }

    private byte[] getResultCurrentValue(final Result result) {

        final NavigableMap<byte[], byte[]> columnMap = result.getFamilyMap(this.getFamilyNameBytes());

        // ColumnMap should not be null at this point, but check just in case
        if (columnMap == null)
            return null;
        else
            return columnMap.get(this.getColumnNameBytes());
    }

    private void assignCalculation(final HConnectionImpl conn,
                                   final Object obj,
                                   final Result result) throws HBqlException {
        // If it is a calculation, then assign according to the AS name
        final String name = this.getAsName();
        final ColumnAttrib attrib = this.getResultAccessor().getColumnAttribByName(name);

        final Object elementValue = this.getValue(conn, result);

        if (attrib == null) {
            // Find value in results and assign the byte[] value to Record, but bail on Annotated object because
            // it cannot deal with unknown/unmapped values
            if (!(obj instanceof HRecord))
                return;

            ((HRecordImpl)obj).setCurrentValue(name, 0, elementValue, false);
        }
        else {
            attrib.setCurrentValue(obj, 0, elementValue);
        }
    }

    public void assignSelectValue(final HConnectionImpl conn,
                                  final Object obj,
                                  final int maxVerions,
                                  final Result result) throws HBqlException {

        if (obj instanceof HRecordImpl) {
            final HRecordImpl record = (HRecordImpl)obj;
            record.addNameToPositionList(this.getSelectName());
        }

        if (this.isAKeyValue())
            return;

        // If it is a calculation, take care of it and then bail since calculations have no history
        if (!this.isADelegateColumnReference()) {
            this.assignCalculation(conn, obj, result);
            return;
        }

        final TableMapping tableMapping = this.getTableMapping();

        // Column reference is not known to mapping, so just assign byte[] value
        if (this.getColumnAttrib() == null) {
            final ColumnAttrib unMappedAttrib = tableMapping.getUnMappedAttrib(this.getFamilyName());
            if (unMappedAttrib != null) {
                final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
                unMappedAttrib.setUnMappedCurrentValue(obj, this.getSelectName(), b);
            }
        }
        else {
            if (this.getColumnAttrib().isACurrentValue()) {
                final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());
                this.getColumnAttrib().setCurrentValue(obj, 0, b);
            }
        }

        // Now assign versions if they were requested. Do not process if it doesn't support version values
        if (maxVerions > 1) {

            // Bail if a known column is not a version attrib
            if (this.getColumnAttrib() != null && !this.getColumnAttrib().isAVersionValue())
                return;

            final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
            final NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = familyMap.get(this.getFamilyNameBytes());

            if (columnMap == null)
                return;

            final NavigableMap<Long, byte[]> timeStampMap = columnMap.get(this.getColumnNameBytes());

            if (this.getColumnAttrib() == null) {
                final ColumnAttrib unMappedAttrib = tableMapping.getUnMappedAttrib(this.getFamilyName());
                if (unMappedAttrib != null)
                    unMappedAttrib.setUnMappedVersionMap(obj, this.getSelectName(), timeStampMap);
            }
            else {
                final Map<Long, Object> mapVal = this.getColumnAttrib().getVersionMap(obj);
                for (final Long timestamp : timeStampMap.keySet()) {
                    final byte[] b = timeStampMap.get(timestamp);
                    final Object val = this.getColumnAttrib().getValueFromBytes(obj, b);
                    mapVal.put(timestamp, val);
                }
            }
        }
    }

    public AggregateValue newAggregateValue() throws HBqlException {
        return new AggregateValue(this.getSelectName(), this);
    }

    public Object getValue(final HConnectionImpl conn, final Result result) throws HBqlException {
        try {
            return this.evaluate(conn, 0, this.allowColumns(), false, result);
        }
        catch (ResultMissingColumnException e) {
            return null;
        }
        catch (NullColumnValueException e) {
            return null;
        }
    }

    public String asString() {
        return this.getGenericValue().asString();
    }

    public boolean useResultData() {
        return true;
    }

    public boolean allowColumns() {
        return true;
    }
}
