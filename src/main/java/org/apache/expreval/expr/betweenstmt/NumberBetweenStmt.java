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

package org.apache.expreval.expr.betweenstmt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberBetweenStmt extends GenericBetweenStmt {

    private static final Log LOG = LogFactory.getLog(NumberBetweenStmt.class);

    public NumberBetweenStmt(final GenericValue arg0,
                             final boolean not,
                             final GenericValue arg1,
                             final GenericValue arg2) {
        super(ExpressionType.NUMBERBETWEEN, not, arg0, arg1, arg2);
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        final Object obj0 = this.getExprArg(0).getValue(conn, object);
        final Object obj1 = this.getExprArg(1).getValue(conn, object);
        final Object obj2 = this.getExprArg(2).getValue(conn, object);

        this.validateNumericArgTypes(obj0, obj1, obj2);

        final boolean retval;

        if (!this.useDecimal()) {
            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();
            final long val2 = ((Number)obj2).longValue();

            retval = val0 >= val1 && val0 <= val2;
        }
        else {
            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();
            final double val2 = ((Number)obj2).doubleValue();

            retval = val0 >= val1 && val0 <= val2;
        }

        return (this.isNot()) ? !retval : retval;
    }

    public Filter getFilter() throws HBqlException {

        this.validateArgsForBetweenFilter();

        final GenericColumn<? extends GenericValue> column = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
        final Object lowerConstant = this.getConstantValue(1);
        final Object upperConstant = this.getConstantValue(2);
        final WritableByteArrayComparable comparator;

        this.validateNumericArgTypes(lowerConstant, upperConstant);

        if (!this.useDecimal()) {
            final long lowerVal = ((Number)lowerConstant).longValue();
            final long upperVal = ((Number)upperConstant).longValue();
            comparator = new LongBetweenComparable(column.getColumnAttrib().getFieldType(), lowerVal, upperVal);
        }
        else {
            final double lowerVal = ((Number)lowerConstant).doubleValue();
            final double upperVal = ((Number)upperConstant).doubleValue();
            comparator = new DoubleBetweenComparable(column.getColumnAttrib().getFieldType(), lowerVal, upperVal);
        }

        return this.newSingleColumnValueFilter(column.getColumnAttrib(), CompareFilter.CompareOp.EQUAL, comparator);
    }

    private static abstract class NumberBetweenComparable<T> extends GenericBetweenComparable<T> {

        private FieldType fieldType;

        protected void setFieldType(final FieldType fieldType) {
            this.fieldType = fieldType;
        }

        protected FieldType getFieldType() {
            return this.fieldType;
        }
    }

    private static class LongBetweenComparable extends NumberBetweenComparable<Long> {

        public LongBetweenComparable() {
        }

        public LongBetweenComparable(final FieldType fieldType, final long lowerValue, final long upperValue) {
            this.setFieldType(fieldType);
            this.setLowerValue(lowerValue);
            this.setUpperValue(upperValue);
        }

        public int compareTo(final byte[] bytes) {

            try {
                long val = IO.getSerialization().getNumberFromBytes(this.getFieldType(), bytes).longValue();
                final double lowerVal = this.getLowerValue();
                final double upperVal = this.getUpperValue();
                return (val >= lowerVal && val <= upperVal) ? 0 : 1;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                Utils.logException(LOG, e);
                return 1;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.getFieldType().name());
            dataOutput.writeLong(this.getLowerValue());
            dataOutput.writeLong(this.getUpperValue());
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.setFieldType(FieldType.valueOf(dataInput.readUTF()));
            this.setLowerValue(dataInput.readLong());
            this.setUpperValue(dataInput.readLong());
        }
    }

    private static class DoubleBetweenComparable extends NumberBetweenComparable<Double> {

        public DoubleBetweenComparable() {
        }

        public DoubleBetweenComparable(final FieldType fieldType, final double lowerValue, final double upperValue) {
            this.setFieldType(fieldType);
            this.setLowerValue(lowerValue);
            this.setUpperValue(upperValue);
        }

        public int compareTo(final byte[] bytes) {

            try {
                double val = IO.getSerialization().getNumberFromBytes(this.getFieldType(), bytes).doubleValue();
                final double lowerVal = this.getLowerValue();
                final double upperVal = this.getUpperValue();
                return (val >= lowerVal && val <= upperVal) ? 0 : 1;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                Utils.logException(LOG, e);
                return 1;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.getFieldType().name());
            dataOutput.writeDouble(this.getLowerValue());
            dataOutput.writeDouble(this.getUpperValue());
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.setFieldType(FieldType.valueOf(dataInput.readUTF()));
            this.setLowerValue(dataInput.readDouble());
            this.setUpperValue(dataInput.readDouble());
        }
    }
}