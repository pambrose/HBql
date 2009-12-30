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

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.statement.NonStatement;

public class ExpressionTree extends MultipleExpressionContext {

    private static FunctionTypeSignature exprSig = new FunctionTypeSignature(BooleanValue.class, BooleanValue.class);

    private boolean useResultData = false;
    private boolean allowColumns = true;

    private Mapping embeddedMapping = null;

    public ExpressionTree() {
    }

    private ExpressionTree(final GenericValue rootValue) {
        super(exprSig, rootValue);
    }

    public static ExpressionTree newExpressionTree(final Mapping mapping, final GenericValue booleanValue) {
        final ExpressionTree tree = new ExpressionTree(booleanValue == null ? new BooleanLiteral(true) : booleanValue);
        // This is stashed until validate() is called.  This avoids throwing HBqlException in parser
        tree.embeddedMapping = mapping;
        return tree;
    }

    // This is not done in newExpressionTree() because that is called from the parser
    public void setEmbeddedMapping() throws HBqlException {
        if (this.embeddedMapping != null)
            this.setStatementContext(new NonStatement(this.embeddedMapping, null));
    }

    public Boolean evaluate(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {
        return (Boolean)this.evaluate(conn, 0, this.allowColumns(), false, object);
    }

    private GenericValue getGenericValue() {
        return this.getGenericValue(0);
    }

    public Filter getFilter() throws HBqlException {

        this.validateTypes(true, true);
        this.optimize();

        return this.getGenericValue().getFilter();
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

    public boolean allowColumns() {
        return this.allowColumns;
    }

    public void setAllowColumns(final boolean allowColumns) {
        this.allowColumns = allowColumns;
    }
}