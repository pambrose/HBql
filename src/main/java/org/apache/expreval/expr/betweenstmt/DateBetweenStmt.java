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

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.mapping.FieldType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class DateBetweenStmt extends GenericBetweenStmt {

    public DateBetweenStmt(final GenericValue expr,
                           final boolean not,
                           final GenericValue lower,
                           final GenericValue upper) {
        super(ExpressionType.DATEBETWEEN, not, expr, lower, upper);
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        final long dateval = (Long)this.getExprArg(0).getValue(conn, object);
        final boolean retval = dateval >= (Long)this.getExprArg(1).getValue(conn, object)
                               && dateval <= (Long)this.getExprArg(2).getValue(conn, object);

        return (this.isNot()) ? !retval : retval;
    }

    public Filter getFilter() throws HBqlException {

        this.validateArgsForBetween();

        final GenericColumn<? extends GenericValue> column = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
        final Long lowerVal = (Long)this.getConstantValue(1);
        final Long upperVal = (Long)this.getConstantValue(2);

        return this.newSingleColumnValueFilter(column.getColumnAttrib(),
                                               CompareFilter.CompareOp.EQUAL,
                                               new DateComparable(lowerVal, upperVal));
    }

    private static class DateComparable extends GenericComparable<Long> {

        public DateComparable() {
        }

        public DateComparable(final Long lowerValue, final Long upperValue) {
            this.setLowerValue(lowerValue);
            this.setUpperValue(upperValue);
        }

        public int compareTo(final byte[] bytes) {
            try {
                Date dateValue = (Date)IO.getSerialization().getScalarFromBytes(FieldType.DateType, bytes);
                long val = dateValue.getTime();
                final long lowerVal = this.getLowerValue();
                final long upperVal = this.getUpperValue();
                return (val >= lowerVal && val <= upperVal) ? 0 : 1;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                return 0;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(this.getLowerValue());
            dataOutput.writeLong(this.getUpperValue());
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.setLowerValue(dataInput.readLong());
            this.setUpperValue(dataInput.readLong());
        }
    }
}