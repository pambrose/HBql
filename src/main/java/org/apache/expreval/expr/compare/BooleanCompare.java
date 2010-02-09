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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.filter.RecordFilterList;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.InvalidServerFilterException;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.util.List;

public class BooleanCompare extends GenericCompare implements BooleanValue {

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
                boolean val0 = (Boolean)this.getValue(0, conn, object);
                boolean val1 = (Boolean)this.getValue(1, conn, object);
                return val0 == val1;
            }
            case NOTEQ: {
                boolean val0 = (Boolean)this.getValue(0, conn, object);
                boolean val1 = (Boolean)this.getValue(1, conn, object);
                return val0 != val1;
            }
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
    }

    public Filter getFilter() throws HBqlException {

        final Filter filter0 = this.getExprArg(0).getFilter();
        final Filter filter1 = this.getExprArg(1).getFilter();

        switch (this.getOperator()) {
            case OR: {
                if (filter0 instanceof RecordFilterList) {
                    if (((RecordFilterList)filter0).getOperator() == RecordFilterList.Operator.MUST_PASS_ONE) {
                        ((RecordFilterList)filter0).addFilter(filter1);
                        return filter0;
                    }
                }

                final List<Filter> filterList = Lists.newArrayList(filter0, filter1);
                return new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ONE, filterList);
            }
            case AND: {
                if (filter0 instanceof RecordFilterList) {
                    if (((RecordFilterList)filter0).getOperator() == RecordFilterList.Operator.MUST_PASS_ALL) {
                        ((RecordFilterList)filter0).addFilter(filter1);
                        return filter0;
                    }
                }

                final List<Filter> filterList = Lists.newArrayList(filter0, filter1);
                return new RecordFilterList(RecordFilterList.Operator.MUST_PASS_ALL, filterList);
            }
            case EQ: {
                throw new InvalidServerFilterException();
            }
            case NOTEQ: {
                throw new InvalidServerFilterException();
            }
            default:
                throw new HBqlException("Invalid operator: " + this.getOperator());
        }
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
}
