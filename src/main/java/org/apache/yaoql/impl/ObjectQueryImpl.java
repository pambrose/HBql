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

package org.apache.yaoql.impl;

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.parser.HBqlShell;
import org.apache.hadoop.hbase.hbql.schema.ReflectionSchema;
import org.apache.yaoql.client.ObjectQuery;
import org.apache.yaoql.client.ObjectQueryListener;
import org.apache.yaoql.client.ObjectResults;

import java.util.Collection;
import java.util.List;

public class ObjectQueryImpl<T> extends ParameterBinding implements ObjectQuery<T> {

    final String query;
    List<ObjectQueryListener<T>> listeners = null;

    public ObjectQueryImpl(final String query) {
        this.query = query;
    }

    public void addListener(final ObjectQueryListener<T> listener) {
        if (this.getListeners() == null)
            this.listeners = Lists.newArrayList();

        this.getListeners().add(listener);
    }

    private List<ObjectQueryListener<T>> getListeners() {
        return this.listeners;
    }

    public void clearListeners() {
        if (this.getListeners() != null)
            this.getListeners().clear();
    }

    public String getQuery() {
        return this.query;
    }

    public ExpressionTree getExpressionTree(final Collection<T> objects) throws HBqlException {

        if (objects == null || objects.size() == 0) {
            final ExpressionTree expressionTree = ExpressionTree.newExpressionTree(true);
            expressionTree.setSchemaAndContext(null);
            return expressionTree;
        }

        // Grab the first object to derive the schema
        final Object obj = objects.iterator().next();
        final ReflectionSchema schema = ReflectionSchema.getReflectionSchema(obj);
        final ExpressionTree expressionTree = HBqlShell.parseWhereExpression(this.getQuery(), schema);
        this.applyParameters(expressionTree);
        return expressionTree;
    }

    public ObjectResults<T> getResults(final Collection<T> objs) throws HBqlException {

        final ObjectResults<T> retval = new ObjectResults<T>(this, objs);

        if (this.getListeners() != null && this.getListeners().size() > 0) {

            for (final ObjectQueryListener<T> listener : this.getListeners())
                listener.onQueryInit();

            for (final T val : retval) {
                for (final ObjectQueryListener<T> listener : this.getListeners())
                    listener.onEachObject(val);
            }

            for (final ObjectQueryListener<T> listener : this.getListeners())
                listener.onQueryComplete();
        }

        return retval;
    }

    public List<T> getResultList(final Collection<T> objs) throws HBqlException {

        final List<T> retval = Lists.newArrayList();

        final ObjectResults<T> results = this.getResults(objs);

        for (T val : results)
            retval.add(val);

        return retval;
    }
}