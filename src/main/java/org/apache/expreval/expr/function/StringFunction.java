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

package org.apache.expreval.expr.function;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.Util;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.util.List;

public class StringFunction extends GenericFunction implements StringValue {

    public StringFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public String getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                   ResultMissingColumnException,
                                                                                   NullColumnValueException {
        switch (this.getFunctionType()) {

            case TRIM: {
                final String val = (String)this.getExprArg(0).getValue(conn, object);
                return val.trim();
            }

            case LOWER: {
                final String val = (String)this.getExprArg(0).getValue(conn, object);
                return val.toLowerCase();
            }

            case UPPER: {
                final String val = (String)this.getExprArg(0).getValue(conn, object);
                return val.toUpperCase();
            }

            case CONCAT: {
                final String v1 = (String)this.getExprArg(0).getValue(conn, object);
                final String v2 = (String)this.getExprArg(1).getValue(conn, object);
                return v1 + v2;
            }

            case REPLACE: {
                final String v1 = (String)this.getExprArg(0).getValue(conn, object);
                final String v2 = (String)this.getExprArg(1).getValue(conn, object);
                final String v3 = (String)this.getExprArg(2).getValue(conn, object);
                return v1.replace(v2, v3);
            }

            case SUBSTRING: {
                final String val = (String)this.getExprArg(0).getValue(conn, object);
                final int begin = ((Number)this.getExprArg(1).getValue(conn, object)).intValue();
                final int length = ((Number)this.getExprArg(2).getValue(conn, object)).intValue();
                return val.substring(begin, begin + length);
            }

            case ZEROPAD: {
                final int num = ((Number)this.getExprArg(0).getValue(conn, object)).intValue();
                final int width = ((Number)this.getExprArg(1).getValue(conn, object)).intValue();
                return Util.getZeroPaddedNonNegativeNumber(num, width);
            }

            case REPEAT: {
                final String val = (String)this.getExprArg(0).getValue(conn, object);
                final int cnt = ((Number)this.getExprArg(1).getValue(conn, object)).intValue();
                final StringBuilder sbuf = new StringBuilder();
                for (int i = 0; i < cnt; i++)
                    sbuf.append(val);
                return sbuf.toString();
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}