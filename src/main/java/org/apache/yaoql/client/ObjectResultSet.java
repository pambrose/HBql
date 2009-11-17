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

package org.apache.yaoql.client;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.ResultsIterator;
import org.apache.yaoql.impl.ObjectQueryImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ObjectResultSet<T> implements Iterable<T> {

    final ObjectQueryImpl<T> objectQuery;
    final Collection<T> objects;

    public ObjectResultSet(final ObjectQueryImpl<T> objectQuery, final Collection<T> objects) {
        this.objectQuery = objectQuery;
        this.objects = objects;
    }

    private ObjectQueryImpl<T> getObjectQuery() {
        return this.objectQuery;
    }

    private Collection<T> getObjects() {
        return this.objects;
    }

    public Iterator<T> iterator() {

        try {
            return new ResultsIterator<T>(-1L) {

                // In theory, this should be done only once and in ObjectQuery, but
                // since it requires the objects to get the schema, I do it here
                final ExpressionTree expressionTree = getObjectQuery().getExpressionTree(getObjects());

                private Iterator<T> objectIter = null;

                // Prime the iterator with the first value
                private T nextObject = fetchNextObject();

                private Iterator<T> getObjectIter() {
                    return this.objectIter;
                }

                private void setObjectIter(final Iterator<T> objectIter) {
                    this.objectIter = objectIter;
                }

                private ExpressionTree getExpressionTree() {
                    return this.expressionTree;
                }

                protected T fetchNextObject() throws HBqlException {

                    if (getObjectIter() == null)
                        setObjectIter(getObjects().iterator());

                    while (this.getObjectIter().hasNext()) {
                        final T val = this.getObjectIter().next();
                        try {
                            if (this.getExpressionTree() == null || this.getExpressionTree().evaluate(val))
                                return val;
                        }
                        catch (ResultMissingColumnException e) {
                            // Just skip and do nothing
                        }
                    }

                    return null;
                }

                protected T getNextObject() {
                    return this.nextObject;
                }

                protected void setNextObject(final T nextObject, final boolean fromExceptionCatch) {
                    this.nextObject = nextObject;
                }
            };
        }
        catch (HBqlException e) {
            e.printStackTrace();
        }

        return new Iterator<T>() {

            public boolean hasNext() {
                return false;
            }

            public T next() {
                throw new NoSuchElementException();
            }

            public void remove() {

            }
        };
    }
}
