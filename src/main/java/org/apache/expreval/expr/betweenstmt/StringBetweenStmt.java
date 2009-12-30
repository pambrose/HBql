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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringBetweenStmt extends GenericBetweenStmt {

    public StringBetweenStmt(final GenericValue arg0,
                             final boolean not,
                             final GenericValue arg1,
                             final GenericValue arg2) {
        super(ExpressionType.STRINGBETWEEN, not, arg0, arg1, arg2);
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        final String val = (String)this.getExprArg(0).getValue(conn, object);
        final String lowerVal = (String)this.getExprArg(1).getValue(conn, object);
        final String upperVal = (String)this.getExprArg(2).getValue(conn, object);

        final boolean retval = val.compareTo(lowerVal) >= 0 && val.compareTo(upperVal) <= 0;

        return (this.isNot()) ? !retval : retval;
    }

    public Filter getFilter() throws HBqlException {

        this.validateArgsForBetween();

        final GenericColumn<? extends GenericValue> column = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
        final String lowerVal = (String)this.getConstantValue(1);
        final String upperVal = (String)this.getConstantValue(2);

        return this.newSingleColumnValueFilter(column.getColumnAttrib(),
                                               CompareFilter.CompareOp.EQUAL,
                                               new StringComparable(lowerVal, upperVal));
    }

    private static class StringComparable extends GenericComparable<String> {

        public StringComparable() {
        }

        public StringComparable(final String lowerValue, final String upperValue) {
            this.setLowerValue(lowerValue);
            this.setUpperValue(upperValue);
        }

        public int compareTo(final byte[] bytes) {

            try {
                final String val = IO.getSerialization().getStringFromBytes(bytes);
                final String lowerVal = this.getLowerValue();
                final String upperVal = this.getUpperValue();
                return (val.compareTo(lowerVal) >= 0 && val.compareTo(upperVal) <= 0) ? 0 : 1;
            }
            catch (HBqlException e) {
                e.printStackTrace();
                return 0;
            }
        }

        public void write(final DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.getLowerValue());
            dataOutput.writeUTF(this.getUpperValue());
        }

        public void readFields(final DataInput dataInput) throws IOException {
            this.setLowerValue(dataInput.readUTF());
            this.setUpperValue(dataInput.readUTF());
        }
    }
}