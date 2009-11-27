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

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.TypeException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.Mapping;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;

import java.util.List;

public class BooleanFunction extends Function implements BooleanValue {

    private Mapping mapping = null;

    public BooleanFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        switch (this.getFunctionType()) {

            case DEFINEDINROW: {
                if (!(this.getArg(0) instanceof DelegateColumn))
                    throw new TypeException("Argument should be a column reference in: " + this.asString());
            }
        }

        return BooleanValue.class;
        //return super.validateTypes(parentExpr, allowCollections);
    }

    public void setExpressionContext(final MultipleExpressionContext context) throws HBqlException {
        super.setExpressionContext(context);
        this.mapping = context.getHBaseTableMapping();
    }

    public Boolean getValue(final HConnectionImpl connection,
                            final Object object) throws HBqlException, ResultMissingColumnException {

        switch (this.getFunctionType()) {

            case RANDOMBOOLEAN: {
                return Function.randomVal.nextBoolean();
            }

            case DEFINEDINROW: {
                try {
                    this.getArg(0).getValue(connection, object);
                    return true;
                }
                catch (ResultMissingColumnException e) {
                    return false;
                }
            }

            case EVAL: {
                final String exprStr = (String)this.getArg(0).getValue(connection, object);
                final StatementContext statementContext = this.getExpressionContext().getStatementContext();
                final ExpressionTree expressionTree = ParserUtil.parseWhereExpression(exprStr, statementContext);
                return expressionTree.evaluate(connection, object);
            }

            case MAPPINGEXISTS: {
                if (connection == null) {
                    return false;
                }
                else {
                    final String mappingName = (String)this.getArg(0).getValue(connection, null);
                    return connection.mappingExists(mappingName);
                }
            }

            case TABLEEXISTS: {
                if (connection == null) {
                    return false;
                }
                else {
                    final String tableName = (String)this.getArg(0).getValue(connection, null);
                    return connection.tableExists(tableName);
                }
            }

            case TABLEENABLED: {
                if (connection == null) {
                    return false;
                }
                else {
                    final String tableName = (String)this.getArg(0).getValue(connection, null);
                    return connection.tableEnabled(tableName);
                }
            }

            case FAMILYEXISTS: {
                if (connection == null) {
                    return false;
                }
                else {
                    final String tableName = (String)this.getArg(0).getValue(connection, null);
                    final String familyName = (String)this.getArg(1).getValue(connection, null);
                    try {
                        return connection.familyExists(tableName, familyName);
                    }
                    catch (HBqlException e) {
                        // return false if table doesn't exist
                        return false;
                    }
                }
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}