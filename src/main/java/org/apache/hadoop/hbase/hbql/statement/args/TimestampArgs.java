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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.IOException;

public class TimestampArgs extends SelectArgs {

    final boolean singleValue;

    public TimestampArgs(final GenericValue arg0, final GenericValue arg1) {
        super(SelectArgs.Type.TIMESTAMPRANGE, arg0, arg1);
        this.singleValue = arg0 == arg1;
    }

    private long getLower() throws HBqlException {
        return (Long)this.evaluateConstant(0, false, null);
    }

    private long getUpper() throws HBqlException {
        return (Long)this.evaluateConstant(1, false, null);
    }

    private boolean isSingleValue() {
        return this.singleValue;
    }

    public String asString() {
        if (this.isSingleValue())
            return "TIMESTAMP " + this.getGenericValue(0).asString();
        else
            return "TIMESTAMP RANGE " + this.getGenericValue(0).asString() + " TO "
                   + this.getGenericValue(1).asString();
    }

    public void setTimeStamp(final Get get) throws HBqlException {
        try {
            if (this.isSingleValue())
                get.setTimeStamp(this.getLower());
            else
                get.setTimeRange(this.getLower(), this.getUpper());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public void setTimeStamp(final Scan scan) throws HBqlException {
        try {
            if (this.isSingleValue())
                scan.setTimeStamp(this.getLower());
            else
                scan.setTimeRange(this.getLower(), this.getUpper());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }
}