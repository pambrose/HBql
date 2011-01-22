/*
 * Copyright (c) 2011.  The Apache Software Foundation
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
import org.apache.hadoop.hbase.hbql.mapping.MappingContext;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.yaoql.impl.ParameterBinding;
import org.apache.yaoql.impl.ReflectionMapping;

import java.util.concurrent.atomic.AtomicBoolean;

public class ObjectQueryPredicate<T> extends ParameterBinding implements Predicate<T> {

    private final String query;
    private ExpressionTree expressionTree = null;
    private AtomicBoolean  initialized    = new AtomicBoolean(false);

    public ObjectQueryPredicate(final String query) {
        this.query = query;
    }

    public void reset() {
        this.initialized.set(false);
    }

    public String getQuery() {
        return this.query;
    }

    private ExpressionTree getExpressionTree() {
        return this.expressionTree;
    }

    private boolean isInitialized() {
        return this.initialized.get();
    }

    public boolean apply(final T obj) {

        try {
            if (!this.isInitialized()) {
                final ReflectionMapping mapping = ReflectionMapping.getReflectionMapping(obj);
                final MappingContext mappingContext = new MappingContext(mapping);
                this.expressionTree = ParserUtil.parseWhereExpression(this.query, mappingContext);
                this.applyParameters(this.getExpressionTree());
                this.initialized.set(true);
            }

            return this.getExpressionTree().evaluate(null, obj);
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
