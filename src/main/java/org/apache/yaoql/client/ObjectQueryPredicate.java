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

package org.apache.yaoql.client;

import com.google.common.base.Predicate;
import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.statement.StatementContext;
import org.apache.yaoql.impl.ParameterBinding;
import org.apache.yaoql.impl.ReflectionMapping;

public class ObjectQueryPredicate<T> extends ParameterBinding implements Predicate<T> {

    private final String query;
    private ExpressionTree expressionTree;
    private boolean initialized = false;

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    public void reset() {
        this.initialized = false;
    }

    public String getQuery() {
        return this.query;
    }

    public boolean apply(final T obj) {

        try {
            if (!initialized) {
                final ReflectionMapping mapping = ReflectionMapping.getReflectionMapping(obj);
                final StatementContext statementContext = new StatementContext(mapping);
                this.expressionTree = ParserUtil.parseWhereExpression(this.query, statementContext);
                this.applyParameters(this.expressionTree);
                initialized = true;
            }

            return expressionTree.evaluate(null, obj);
        }
        catch (ResultMissingColumnException e) {
            // Not possible
            return false;
        }
        catch (NullColumnValueException e) {
            // Not possible
            return false;
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return false;
        }
    }
}
