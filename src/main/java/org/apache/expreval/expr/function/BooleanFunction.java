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

package org.apache.expreval.expr.function;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.var.DelegateColumn;
import org.apache.hadoop.hbase.hbql.client.AsyncExecutorManager;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.InvalidTypeException;
import org.apache.hadoop.hbase.hbql.client.QueryExecutorPoolManager;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.MappingContext;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;

import java.util.List;

public class BooleanFunction extends GenericFunction implements BooleanValue {


    public BooleanFunction(final FunctionType functionType, final List<GenericValue> exprs) {
        super(functionType, exprs);
    }

    public Class<? extends GenericValue> validateTypes(final GenericValue parentExpr,
                                                       final boolean allowCollections) throws HBqlException {

        super.validateTypes(parentExpr, allowCollections);

        switch (this.getFunctionType()) {

            case DEFINEDINROW: {
                if (!(this.getExprArg(0) instanceof DelegateColumn))
                    throw new InvalidTypeException("Argument should be a column reference in: " + this.asString());
            }
        }

        return BooleanValue.class;
    }

    public Boolean getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                    ResultMissingColumnException,
                                                                                    NullColumnValueException {

        switch (this.getFunctionType()) {

            case RANDOMBOOLEAN:
                return GenericFunction.randomVal.nextBoolean();

            case DEFINEDINROW:
                try {
                    this.getExprArg(0).getValue(conn, object);
                    return true;
                }
                catch (ResultMissingColumnException e) {
                    return false;
                }

            case EVAL:
                final String exprStr = (String)this.getExprArg(0).getValue(conn, object);
                final MappingContext mappingContext = this.getExpressionContext().getMappingContext();
                final ExpressionTree expressionTree = ParserUtil.parseWhereExpression(exprStr, mappingContext);
                return expressionTree.evaluate(conn, object);

            case MAPPINGEXISTS:
                if (conn == null) {
                    return false;
                }
                else {
                    final String mappingName = (String)this.getExprArg(0).getValue(conn, null);
                    return conn.mappingExists(mappingName);
                }

            case TABLEEXISTS:
                if (conn == null) {
                    return false;
                }
                else {
                    final String tableName = (String)this.getExprArg(0).getValue(conn, null);
                    return conn.tableExists(tableName);
                }

            case TABLEENABLED:
                if (conn == null) {
                    return false;
                }
                else {
                    final String tableName = (String)this.getExprArg(0).getValue(conn, null);
                    return conn.tableEnabled(tableName);
                }

            case FAMILYEXISTSFORTABLE:
                if (conn == null) {
                    return false;
                }
                else {
                    final String familyName = (String)this.getExprArg(0).getValue(conn, null);
                    final String tableName = (String)this.getExprArg(1).getValue(conn, null);
                    try {
                        return conn.familyExistsForTable(familyName, tableName);
                    }
                    catch (HBqlException e) {
                        // return false if table doesn't exist
                        return false;
                    }
                }

            case FAMILYEXISTSFORMAPPING:
                if (conn == null) {
                    return false;
                }
                else {
                    final String familyName = (String)this.getExprArg(0).getValue(conn, null);
                    final String mappingName = (String)this.getExprArg(1).getValue(conn, null);
                    try {
                        return conn.familyExistsForMapping(familyName, mappingName);
                    }
                    catch (HBqlException e) {
                        // return false if table doesn't exist
                        return false;
                    }
                }

            case INDEXEXISTSFORTABLE:
                if (conn == null) {
                    return false;
                }
                else {
                    final String indexName = (String)this.getExprArg(0).getValue(conn, null);
                    final String tableName = (String)this.getExprArg(1).getValue(conn, null);
                    try {
                        return conn.indexExistsForTable(indexName, tableName);
                    }
                    catch (HBqlException e) {
                        // return false if index doesn't exist
                        return false;
                    }
                }

            case INDEXEXISTSFORMAPPING:
                if (conn == null) {
                    return false;
                }
                else {
                    final String indexName = (String)this.getExprArg(0).getValue(conn, null);
                    final String mappingName = (String)this.getExprArg(1).getValue(conn, null);
                    try {
                        return conn.indexExistsForMapping(indexName, mappingName);
                    }
                    catch (HBqlException e) {
                        // return false if index doesn't exist
                        return false;
                    }
                }

            case ASYNCEXECUTORPOOLEXISTS: {
                final String poolName = (String)this.getExprArg(0).getValue(null, null);
                return AsyncExecutorManager.asyncExecutorExists(poolName);
            }

            case QUERYEXECUTORPOOLEXISTS: {
                final String poolName = (String)this.getExprArg(0).getValue(null, null);
                return QueryExecutorPoolManager.queryExecutorPoolExists(poolName);
            }

            default:
                throw new HBqlException("Invalid function: " + this.getFunctionType());
        }
    }
}