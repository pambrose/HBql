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
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.util.List;

public abstract class GenericCase extends DelegateStmt<GenericCase> {

    private final List<GenericCaseWhen> whenExprList;
    private GenericCaseElse elseExpr;

    protected GenericCase(final ExpressionType type, final List<GenericCaseWhen> whenExprList, final GenericCaseElse elseExpr) {
        super(type);
        this.whenExprList = whenExprList;
        this.elseExpr = elseExpr;
    }

    public Object getValue(final Object object) throws HBqlException, ResultMissingColumnException {

        for (final GenericCaseWhen when : this.getWhenExprList()) {
            final boolean predicate = when.getPredicateValue(object);
            if (predicate)
                return when.getValue(object);
        }

        if (this.getElseExpr() != null)
            return this.getElseExpr().getValue(object);

        return null;
    }

    public void reset() {
        for (final GenericCaseWhen when : this.getWhenExprList())
            when.reset();

        if (this.getElseExpr() != null)
            this.getElseExpr().reset();
    }

    public String asString() {

        final StringBuilder sbuf = new StringBuilder();

        sbuf.append("CASE ");

        for (final GenericCaseWhen when : this.getWhenExprList())
            sbuf.append(when.asString());

        if (this.getElseExpr() != null)
            sbuf.append(this.getElseExpr().asString());

        sbuf.append("END");

        return sbuf.toString();
    }


    protected List<GenericCaseWhen> getWhenExprList() {
        return this.whenExprList;
    }

    protected GenericCaseElse getElseExpr() {
        return this.elseExpr;
    }

    protected void setElseExpr(final GenericCaseElse elseExpr) {
        this.elseExpr = elseExpr;
    }
}