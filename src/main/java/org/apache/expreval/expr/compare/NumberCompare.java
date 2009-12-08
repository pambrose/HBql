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

package org.apache.expreval.expr.compare;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberCompare extends GenericCompare {

    public NumberCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {
        return this.validateType(NumberValue.class);
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getValue(0, connection, object);
        final Object obj1 = this.getValue(1, connection, object);

        this.validateNumericArgTypes(obj0, obj1);

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();
            final long val1 = ((Number)obj1).longValue();

            switch (this.getOperator()) {
                case EQ:
                    return val0 == val1;
                case GT:
                    return val0 > val1;
                case GTEQ:
                    return val0 >= val1;
                case LT:
                    return val0 < val1;
                case LTEQ:
                    return val0 <= val1;
                case NOTEQ:
                    return val0 != val1;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();
            final double val1 = ((Number)obj1).doubleValue();

            switch (this.getOperator()) {
                case EQ:
                    return val0 == val1;
                case GT:
                    return val0 > val1;
                case GTEQ:
                    return val0 >= val1;
                case LT:
                    return val0 < val1;
                case LTEQ:
                    return val0 <= val1;
                case NOTEQ:
                    return val0 != val1;
                default:
                    throw new HBqlException("Invalid operator: " + this.getOperator());
            }
        }
    }

    public Filter getFilter() throws HBqlException, ResultMissingColumnException {

        // One of these values must be a single column reference and the other a constant
        this.validateArgsForFilter();

        final GenericColumn columnRef;
        final Object constant;
        final CompareFilter.CompareOp compareOp;
        final WritableByteArrayComparable comparator;

        if (this.getExprArg(0).isAColumnReference()) {
            columnRef = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
            constant = this.getValue(1, null, null);
            compareOp = this.getOperator().getCompareOpLeft();
        }
        else {
            columnRef = ((DelegateColumn)this.getExprArg(1)).getTypedColumn();
            constant = this.getValue(0, null, null);
            compareOp = this.getOperator().getCompareOpRight();
        }

        if (compareOp == null)
            throw new HBqlException("Invalid operator: " + this.getOperator());

        this.validateNumericArgTypes(constant);

        if (!this.useDecimal()) {
            final long val = ((Number)constant).longValue();
            comparator = new LongComparable(val, columnRef.getColumnAttrib().getFieldType());
        }
        else {
            final double val = ((Number)constant).doubleValue();
            comparator = new DoubleComparable(val, columnRef.getColumnAttrib().getFieldType());
        }

        return new SingleColumnValueFilter(columnRef.getColumnAttrib().getFamilyNameAsBytes(),
                                           columnRef.getColumnAttrib().getColumnNameAsBytes(),
                                           compareOp,
                                           comparator);
    }

    public static class LongComparable implements WritableByteArrayComparable {

        long value;
        FieldType fieldType;

        public LongComparable() {
        }

        public LongComparable(final long value, final FieldType fieldType) {
            this.value = value;
            this.fieldType = fieldType;
        }

        public int compareTo(final byte[] bytes) {
            try {
                long columnValue = IO.getSerialization().getNumberFromBytes(this.fieldType, bytes).longValue();
                System.out.println("ZZZ7 comparing " + columnValue + " and " + this.value);
                if (columnValue == this.value)
                    return 0;
                else
                    return (columnValue > this.value) ? -1 : 1;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                return 0;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(this.value);
            dataOutput.writeUTF(this.fieldType.name());
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.value = dataInput.readLong();
            this.fieldType = FieldType.valueOf(dataInput.readUTF());
        }
    }

    public static class DoubleComparable implements WritableByteArrayComparable {

        double value;
        FieldType fieldType;

        public DoubleComparable() {
        }

        public DoubleComparable(final double value, final FieldType fieldType) {
            this.value = value;
            this.fieldType = fieldType;
        }

        public int compareTo(final byte[] bytes) {
            try {
                double columnValue = IO.getSerialization().getNumberFromBytes(this.fieldType, bytes).doubleValue();
                if (columnValue == this.value)
                    return 0;
                else
                    return (columnValue > this.value) ? -1 : 1;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                return 0;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(this.value);
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.value = dataInput.readDouble();
        }
    }
}