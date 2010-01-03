/*
 * Copyright (c) 2010.  The Apache Software Foundation
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

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LikeStmt extends GenericStringPatternStmt {

    private Pattern pattern = null;

    public LikeStmt(final GenericValue arg0, final boolean not, final GenericValue arg1) {
        super(arg0, not, arg1);
    }

    private Pattern getPattern() {
        return this.pattern;
    }

    protected String getFunctionName() {
        return "LIKE";
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        final String val = (String)this.getExprArg(0).getValue(conn, object);

        if (val == null)
            throw new HBqlException("Null string for value in " + this.asString());

        final boolean patternConstant = this.getExprArg(1).isAConstant();

        if (!patternConstant || this.getPattern() == null) {

            final String pvalue = (String)this.getExprArg(1).getValue(conn, object);

            if (pvalue == null)
                throw new HBqlException("Null string for pattern in " + this.asString());

            this.pattern = Pattern.compile(pvalue);
        }

        final Matcher m = this.getPattern().matcher(val);

        final boolean retval = m.matches();

        return (this.isNot()) ? !retval : retval;
    }
}