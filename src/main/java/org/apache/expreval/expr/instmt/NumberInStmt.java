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

package org.apache.expreval.expr.instmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.TypeSupport;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.util.Collection;
import java.util.List;

public class NumberInStmt extends GenericInStmt {

    public NumberInStmt(final GenericValue arg0, final boolean not, final List<GenericValue> inList) {
        super(arg0, not, inList);
    }

    protected boolean evaluateInList(final Object object) throws HBqlException, ResultMissingColumnException {

        final Object obj0 = this.getArg(0).getValue(null, object);

        this.validateNumericArgTypes(obj0);

        if (!this.useDecimal()) {

            final long val0 = ((Number)obj0).longValue();

            for (final GenericValue obj : this.getInList()) {

                // Check if the value returned is a collection
                final Object objval = obj.getValue(null, object);
                if (TypeSupport.isACollection(objval)) {
                    for (final GenericValue genericValue : (Collection<GenericValue>)objval) {
                        if (val0 == ((Number)genericValue.getValue(null, object)).longValue())
                            return true;
                    }
                }
                else {
                    if (val0 == ((Number)objval).longValue())
                        return true;
                }
            }
            return false;
        }
        else {

            final double val0 = ((Number)obj0).doubleValue();

            for (final GenericValue obj : this.getInList()) {

                // Check if the value returned is a collection
                final Object objval = obj.getValue(null, object);
                if (TypeSupport.isACollection(objval)) {
                    for (final GenericValue genericValue : (Collection<GenericValue>)objval) {
                        if (val0 == ((Number)genericValue.getValue(null, object)).doubleValue())
                            return true;
                    }
                }
                else {
                    if (val0 == ((Number)objval).doubleValue())
                        return true;
                }
            }
            return false;
        }
    }
}