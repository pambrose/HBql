/*
 * Copyright (c) 2011.  The Apache Software Foundation
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

package org.apache.expreval.expr.instmt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.expreval.expr.var.GenericColumn;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.InvalidServerFilterException;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class StringInStmt extends GenericInStmt {

    private static final Log LOG = LogFactory.getLog(StringInStmt.class);

    public StringInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException,
                                                                 ResultMissingColumnException,
                                                                 NullColumnValueException {
        final String attribVal = (String)this.getExprArg(0).getValue(null, object);

        for (final GenericValue obj : this.getInList()) {

            // Check if the value returned is a collection
            final Object objval = obj.getValue(null, object);
            if (TypeSupport.isACollection(objval)) {
                for (final GenericValue val : (Collection<GenericValue>)objval) {
                    if (attribVal.equals(val.getValue(null, object)))
                        return true;
                }
            }
            else {
                if (attribVal.equals(objval))
                    return true;
            }
        }
        return false;
    }

    public Filter getFilter() throws HBqlException {

        this.validateArgsForInFilter();

        final GenericColumn<? extends GenericValue> column = ((DelegateColumn)this.getExprArg(0)).getTypedColumn();
        final List<byte[]> inValues = Lists.newArrayList();
        try {
            for (final GenericValue obj : this.getInList()) {
                final byte[] val = Bytes.toBytes((String)obj.getValue(null, null));
                inValues.add(val);
            }
        }
        catch (ResultMissingColumnException e) {
            throw new InvalidServerFilterException();
        }
        catch (NullColumnValueException e) {
            throw new InvalidServerFilterException();
        }

        return this.newSingleColumnValueFilter(column.getColumnAttrib(),
                                               CompareFilter.CompareOp.EQUAL,
                                               new StringInComparable(inValues));
    }

    private static class StringInComparable extends GenericInComparable<byte[]> {

        public StringInComparable() {
        }

        public StringInComparable(final List<byte[]> inValues) {
            this.setInValue(inValues);
        }

        public int compareTo(final byte[] val) {
            for (final byte[] inVal : this.getInValues()) {
                if (Bytes.equals(val, inVal))
                    return 0;
            }
            return 1;
        }

        public void write(final DataOutput dataOutput) throws IOException {
            try {
                final byte[] b = IO.getSerialization().getObjectAsBytes(this.getInValues());
                Bytes.writeByteArray(dataOutput, b);
            }
            catch (HBqlException e) {
                e.printStackTrace();
                Utils.logException(LOG, e);
                throw new IOException("HBqlException: " + e.getCause());
            }
        }

        public void readFields(final DataInput dataInput) throws IOException {
            try {
                final byte[] b = Bytes.readByteArray(dataInput);
                final List<byte[]> inValues = (List<byte[]>)IO.getSerialization().getObjectFromBytes(b);
                this.setInValue(inValues);
            }
            catch (HBqlException e) {
                e.printStackTrace();
                Utils.logException(LOG, e);
                throw new IOException("HBqlException: " + e.getCause());
            }
        }
    }
}