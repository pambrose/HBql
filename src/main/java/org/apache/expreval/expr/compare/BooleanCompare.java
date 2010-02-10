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

package org.apache.expreval.expr.compare;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.Operator;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.hadoop.hbase.client.idx.exp.And;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.client.idx.exp.Or;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.filter.RecordFilterList;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class BooleanCompare extends GenericCompare implements BooleanValue {

    private static final Log LOG = LogFactory.getLog(BooleanCompare.class);

    public BooleanCompare(final GenericValue arg0, final Operator operator, final GenericValue arg1) {
        super(arg0, operator, arg1);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {
        return this.validateType(BooleanValue.class);
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {

        // Do not pre-evalute the OR or AND values. Should evaluate them in place
        switch (this.getOperator()) {
            case OR:
                return (Boolean)this.getValue(0, conn, object) || (Boolean)this.getValue(1, conn, object);
            case AND:
                return (Boolean)this.getValue(0, conn, object) && (Boolean)this.getValue(1, conn, object);
            case EQ: {
                final Object obj0 = this.getValue(0, conn, object);
                final Object obj1 = this.getValue(1, conn, object);

                if (obj0 == null || obj1 == null) {
                    return false;
                }
                else {
                    boolean val0 = (Boolean)obj0;
                    boolean val1 = (Boolean)obj1;
                    return val0 == val1;
                }
            }
            case NOTEQ: {
                final Object obj0 = this.getValue(0, conn, object);
                final Object obj1 = this.getValue(1, conn, object);

                if (obj0 == null || obj1 == null) {
                    return false;
                }
                else {
                    boolean val0 = (Boolean)obj0;
                    boolean val1 = (Boolean)obj1;
                    return val0 != val1;
                }
            }
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }

    public Filter getFilter() throws HBqlException {

        if (this.getOperator() == Operator.OR || this.getOperator() == Operator.AND) {

            final Filter filter0 = this.getExprArg(0).getFilter();
            final Filter filter1 = this.getExprArg(1).getFilter();

            if (this.getOperator() == Operator.OR) {
                if (filter0 instanceof RecordFilterList) {
                    if (((RecordFilterList)filter0).getOperator() == RecordFilterList.Operator.MUST_PASS_ONE) {
                        ((RecordFilterList)filter0).addFilter(filter1);
                        return filter0;
                    }
                }

                final List<Filter> filterList = Lists.newArrayList(filter0, filter1);
                return new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ONE, filterList);
            }
            else {
                if (filter0 instanceof RecordFilterList) {
                    if (((RecordFilterList)filter0).getOperator() == RecordFilterList.Operator.MUST_PASS_ALL) {
                        ((RecordFilterList)filter0).addFilter(filter1);
                        return filter0;
                    }
                }

                final List<Filter> filterList = Lists.newArrayList(filter0, filter1);
                return new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ALL, filterList);
            }
        }

        if (this.getOperator() == Operator.EQ || this.getOperator() == Operator.NOTEQ) {

            this.validateArgsForCompareFilter();

            final GenericColumn<? extends GenericValue> column;
            final Object constant;
            final CompareFilter.CompareOp compareOp;

            if (this.getExprArg(0).isAColumnReference()) {
                column = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
                constant = this.getConstantValue(1);
                compareOp = this.getOperator().getCompareOpLeft();
            }
            else {
                column = ((DelegateColumn)this.getExprArg(1)).getTypedColumn();
                constant = this.getConstantValue(0);
                compareOp = this.getOperator().getCompareOpRight();
            }

            return this.newSingleColumnValueFilter(column.getColumnAttrib(),
                                                   compareOp,
                                                   new BooleanComparable((Boolean)constant));
        }

        throw new HBqlException("Invalid operator: " + this.getOperator());
    }

    public Expression getIndexExpression() throws HBqlException {

        if (this.getOperator() == Operator.OR || this.getOperator() == Operator.AND) {

            final Expression expr0 = this.getExprArg(0).getIndexExpression();
            final Expression expr1 = this.getExprArg(1).getIndexExpression();

            if (this.getOperator() == Operator.OR) {
                if (expr0 instanceof Or) {
                    ((Or)expr0).or(expr1);
                    return expr0;
                }

                return new Or(expr0, expr1);
            }
            else {
                if (expr0 instanceof And) {
                    ((And)expr0).and(expr1);
                    return expr0;
                }

                return new And(expr0, expr1);
            }
        }

        if (this.getOperator() == Operator.EQ) {

            this.validateArgsForCompareFilter();

            final GenericColumn<? extends GenericValue> column;
            final Object constant;
            final Comparison.Operator comparison;

            if (this.getExprArg(0).isAColumnReference()) {
                column = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
                constant = this.getConstantValue(1);
                comparison = this.getOperator().getComparisonLeft();
            }
            else {
                column = ((DelegateColumn)this.getExprArg(1)).getTypedColumn();
                constant = this.getConstantValue(0);
                comparison = this.getOperator().getComparisonRight();
            }

            this.validateNumericArgTypes(constant);

            final FieldType type = column.getColumnAttrib().getFieldType();
            final byte[] compareVal = IO.getSerialization().getScalarAsBytes(type, constant);

            return Expression.comparison(column.getColumnAttrib().getFamilyNameAsBytes(),
                                         column.getColumnAttrib().getColumnNameAsBytes(),
                                         comparison,
                                         compareVal);
        }

        throw new HBqlException("Invalid operator: " + this.getOperator());
    }

    private static class BooleanComparable extends GenericComparable<Boolean> {

        public BooleanComparable() {
        }

        public BooleanComparable(final Boolean value) {
            this.setValue(value);
        }

        public int compareTo(final byte[] bytes) {

            if (this.equalValues(bytes))
                return 0;

            try {
                final Boolean columnValue = IO.getSerialization().getBooleanFromBytes(bytes);
                return (this.getValue().compareTo(columnValue));
            }
            catch (HBqlException e) {
                e.printStackTrace();
                Utils.logException(LOG, e);
                return 1;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeBoolean(this.getValue());
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.setValue(dataInput.readBoolean());

            this.setValueInBytes(FieldType.BooleanType, this.getValue());
        }
    }
}
