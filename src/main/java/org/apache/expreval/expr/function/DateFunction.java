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

package org.apache.expreval.expr.function;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class DateFunction extends GenericFunction implements DateValue {

    public enum ConstantType {
        NOW(0),
        MINDATE(0),
        MAXDATE(Long.MAX_VALUE);

        final long value;

        ConstantType(final long value) {
            this.value = value;
        }

        public long getValue() {
            return this.value;
        }

        public static GenericFunction getFunction(final String functionName) {

            try {
                final ConstantType type = ConstantType.valueOf(functionName.toUpperCase());
                return new DateFunction(type);
            }
            catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    public enum IntervalType {
        MILLI(1),
        SECOND(1000 * MILLI.getIntervalMillis()),
        MINUTE(60 * SECOND.getIntervalMillis()),
        HOUR(60 * MINUTE.getIntervalMillis()),
        DAY(24 * HOUR.getIntervalMillis()),
        WEEK(7 * DAY.getIntervalMillis()),
        YEAR(52 * WEEK.getIntervalMillis());

        private final long intervalMillis;

        IntervalType(final long intervalMillis) {
            this.intervalMillis = intervalMillis;
        }

        public long getIntervalMillis() {
            return intervalMillis;
        }

        public static GenericFunction getFunction(final String functionName, final List<GenericValue> exprList) {

            try {
                final IntervalType type = IntervalType.valueOf(functionName.toUpperCase());
                return new DateFunction(type, exprList);
            }
            catch (IllegalArgumentException e) {
                return null;
            }
        }
    }

    private ConstantType constantType;
    private IntervalType intervalType;
    private DateLiteral dateValue;

    public DateFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public DateFunction(final ConstantType constantType) {
        super(FunctionType.DATECONSTANT, null);
        this.constantType = constantType;
        switch (this.getConstantType()) {
            case NOW:
                this.dateValue = new DateLiteral(DateLiteral.getNow());
                break;
            case MINDATE:
            case MAXDATE:
                this.dateValue = new DateLiteral(constantType.getValue());
                break;
        }
    }

    public DateFunction(final IntervalType intervalType, final List<GenericValue> exprs) {
        super(FunctionType.DATEINTERVAL, exprs);
        this.intervalType = intervalType;
    }

    private ConstantType getConstantType() {
        return this.constantType;
    }

    private IntervalType getIntervalType() {
        return this.intervalType;
    }

    public Long getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                 ResultMissingColumnException,
                                                                                 NullColumnValueException {
        switch (this.getFunctionType()) {

            case DATE: {
                final String datestr = (String)this.getExprArg(0).getValue(conn, object);
                final String pattern = (String)this.getExprArg(1).getValue(conn, object);
                final SimpleDateFormat formatter = new SimpleDateFormat(pattern);

                try {
                    return formatter.parse(datestr).getTime();
                }
                catch (ParseException e) {
                    throw new HBqlException(e.getMessage());
                }
            }

            case DATEINTERVAL: {
                final Number num = (Number)this.getExprArg(0).getValue(conn, object);
                final long val = num.longValue();
                return val * this.getIntervalType().getIntervalMillis();
            }

            case DATECONSTANT: {
                return this.dateValue.getValue(conn, object);
            }

            case RANDOMDATE: {
                return Math.abs(GenericFunction.randomVal.nextLong());
            }

            case LONGTODATE: {
                final Number num = (Number)this.getExprArg(0).getValue(conn, object);
                final long val = num.longValue();
                this.dateValue = new DateLiteral(val);
                return this.dateValue.getValue(conn, object);
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }

    protected String getFunctionName() {
        if (this.isIntervalDate())
            return this.getIntervalType().name();
        else if (this.isConstantDate())
            return this.getConstantType().name();
        else
            return super.getFunctionName();
    }


    public String asString() {
        if (this.isIntervalDate())
            return this.getIntervalType().name() + "(" + this.getExprArg(0).asString() + ")";
        else if (this.isConstantDate())
            return this.getConstantType().name() + "()";
        else
            return super.asString();
    }
}