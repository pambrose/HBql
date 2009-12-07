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

package org.apache.expreval.expr.stringpattern;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

public class ContainsStmt extends GenericStringPatternStmt {

    public ContainsStmt(final GenericValue valueExpr, final boolean not, final GenericValue patternExpr) {
        super(valueExpr, not, patternExpr);
    }

    protected String getFunctionName() {
        return "CONTAINS";
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {

        final String val1 = (String)this.getExprArg(0).getValue(connection, object);
        final String val2 = (String)this.getExprArg(1).getValue(connection, object);

        if (val1 == null)
            throw new HBqlException("Null string for value in " + this.asString());

        if (val2 == null)
            throw new HBqlException("Null string for pattern in " + this.asString());

        final boolean retval = val1.contains(val2);

        return (this.isNot()) ? !retval : retval;
    }
}