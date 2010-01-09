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

package org.apache.yaoql.impl;

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.mapping.MappingContext;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.yaoql.client.ObjectQuery;
import org.apache.yaoql.client.ObjectQueryListener;
import org.apache.yaoql.client.ObjectResultSet;

import java.util.Collection;
import java.util.List;

public class ObjectQueryImpl<T> extends ParameterBinding implements ObjectQuery<T> {

    private final String query;
    private List<ObjectQueryListener<T>> queryListeners = null;

    public ObjectQueryImpl(final String query) {
        this.query = query;
    }

    public void addListener(final ObjectQueryListener<T> listener) {
        if (this.getQueryListeners() == null)
            this.queryListeners = Lists.newArrayList();

        this.getQueryListeners().add(listener);
    }

    private List<ObjectQueryListener<T>> getQueryListeners() {
        return this.queryListeners;
    }

    public void clearListeners() {
        if (this.getQueryListeners() != null)
            this.getQueryListeners().clear();
    }

    private void callOnQueryInit() {
        if (this.getQueryListeners() != null) {
            for (final ObjectQueryListener<T> listener : this.getQueryListeners())
                listener.onQueryStart();
        }
    }

    public T callOnEachObject(T val) {
        if (this.getQueryListeners() != null) {
            for (final ObjectQueryListener<T> listener : this.getQueryListeners())
                listener.onEachObject(val);
        }
        return val;
    }

    public void callOnQueryComplete() {
        if (this.getQueryListeners() != null) {
            for (final ObjectQueryListener<T> listener : this.getQueryListeners())
                listener.onQueryComplete();
        }
    }

    public String getQuery() {
        return this.query;
    }

    public ExpressionTree getExpressionTree(final Collection<T> objects) throws HBqlException {

        final ExpressionTree expressionTree;

        if (objects == null || objects.size() == 0) {
            expressionTree = ExpressionTree.newExpressionTree(null, new BooleanLiteral(true));
            expressionTree.setMappingContext(null);
            expressionTree.setAllowColumns(false);
        }
        else {
            // Grab the first object to derive the mapping
            final Object obj = objects.iterator().next();
            final ReflectionMapping mapping = ReflectionMapping.getReflectionMapping(obj);
            final MappingContext mappingContext = new MappingContext(mapping);
            expressionTree = ParserUtil.parseWhereExpression(this.getQuery(), mappingContext);
            this.applyParameters(expressionTree);
        }

        return expressionTree;
    }

    public ObjectResultSet<T> getResults(final Collection<T> objs) throws HBqlException {

        this.callOnQueryInit();

        return new ObjectResultSet<T>(this, objs);
    }

    public List<T> getResultList(final Collection<T> objs) throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        final ObjectResultSet<T> results = this.getResults(objs);

        for (final T val : results)
            retval.add(val);

        return retval;
    }
}