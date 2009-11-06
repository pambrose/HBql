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

package org.apache.expreval.expr.casestmt;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.DelegateStmt;
import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

public abstract class GenericCaseWhen extends DelegateStmt<GenericCaseWhen> {

    protected GenericCaseWhen(final ExpressionType type,
                              final GenericValue arg0,
                              final GenericValue arg1) {
        super(type, arg0, arg1);
    }

    public boolean getPredicateValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)this.getArg(0).getValue(object);
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {
        return this.getArg(1).getValue(object);
    }

    public String asString() {
        return "WHEN " + this.getArg(0).asString() + " THEN " + this.getArg(1).asString() + " ";
    }
}