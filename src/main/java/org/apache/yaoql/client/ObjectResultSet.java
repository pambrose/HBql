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

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.ResultSetIterator;
import org.apache.hadoop.hbase.hbql.util.NullIterator;
import org.apache.yaoql.impl.ObjectQueryImpl;

import java.util.Collection;
import java.util.Iterator;

public class ObjectResultSet<T> implements Iterable<T> {

    private final ObjectQueryImpl<T> objectQuery;
    private final Collection<T> objects;
    private final ExpressionTree expressionTree;
    private Iterator<T> objectIter = null;

    public ObjectResultSet(final ObjectQueryImpl<T> objectQuery, final Collection<T> objects) throws HBqlException {
        this.objectQuery = objectQuery;
        this.objects = objects;

        // In theory, this should be done only once and in ObjectQuery, but
        // since it requires the objects to get the mapping, I do it here
        this.expressionTree = getObjectQuery().getExpressionTree(getObjects());
    }

    private ObjectQueryImpl<T> getObjectQuery() {
        return this.objectQuery;
    }

    private Collection<T> getObjects() {
        return this.objects;
    }

    private Iterator<T> getObjectIter() {
        return this.objectIter;
    }

    private void setObjectIter(final Iterator<T> objectIter) {
        this.objectIter = objectIter;
    }

    private ExpressionTree getExpressionTree() {
        return this.expressionTree;
    }

    public Iterator<T> iterator() {

        try {
            return new ResultSetIterator<T, Object>(null) {

                protected T fetchNextObject() throws HBqlException {

                    if (getObjectIter() == null)
                        setObjectIter(getObjects().iterator());

                    while (getObjectIter().hasNext()) {
                        final T val = getObjectIter().next();
                        try {
                            if (getExpressionTree() == null || getExpressionTree().evaluate(null, val))
                                return getObjectQuery().callOnEachObject(val);
                        }
                        catch (ResultMissingColumnException e) {
                            // Just skip and do nothing
                        }
                        catch (NullColumnValueException e) {
                            // Just skip and do nothing
                        }
                    }

                    this.markIteratorComplete();
                    return null;
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {
                    this.setNextObject(nextObject);
                }

                protected boolean moreResultsPending() {
                    return false;
                }

                protected Iterator<Result> getNextResultIterator() throws HBqlException {
                    return null;
                }

                protected void iteratorCompleteAction() {
                    getObjectQuery().callOnQueryComplete();
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
            return new NullIterator<T>();
        }
    }
}
