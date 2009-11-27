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

package org.apache.expreval.expr;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.statement.NonStatement;

public class ExpressionTree extends MultipleExpressionContext {

    private static FunctionTypeSignature exprSignature = new FunctionTypeSignature(BooleanValue.class, BooleanValue.class);
    private boolean useResultData = false;

    private ExpressionTree(final GenericValue rootValue) {
        super(exprSignature, rootValue);
    }

    public static ExpressionTree newExpressionTree(final Mapping mapping, final GenericValue booleanValue) {
        final ExpressionTree tree = new ExpressionTree(booleanValue == null ? new BooleanLiteral(true) : booleanValue);
        if (mapping != null)
            tree.setStatementContext(new NonStatement(mapping, null));
        return tree;
    }

    public Boolean evaluate(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {
        return (Boolean)this.evaluate(connection, 0, true, false, object);
    }

    private GenericValue getGenericValue() {
        return this.getGenericValue(0);
    }

    public String asString() {
        return this.getGenericValue().asString();
    }

    public void setUseResultData(final boolean useResultData) {
        this.useResultData = useResultData;
    }

    public boolean useResultData() {
        return this.useResultData;
    }
}