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

package org.apache.expreval.expr.node;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.AggregateValue;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;

import java.io.Serializable;

public interface GenericValue extends Serializable {

    static final long serialVersionUID = 1L;

    void setExpressionContext(MultipleExpressionContext context) throws HBqlException;

    Object getValue(HConnectionImpl connection, Object object) throws HBqlException,
                                                                      ResultMissingColumnException,
                                                                      NullColumnValueException;

    Filter getFilter() throws HBqlException;

    GenericValue getOptimizedValue() throws HBqlException;

    Class<? extends GenericValue> validateTypes(GenericValue parentExpr, boolean allowCollections) throws HBqlException;

    boolean isAConstant();

    boolean isDefaultKeyword();

    boolean isAnAggregateValue();

    void initAggregateValue(AggregateValue aggregateValue) throws HBqlException;

    void applyResultToAggregateValue(AggregateValue aggregateValue, Result result) throws HBqlException,
                                                                                          ResultMissingColumnException,
                                                                                          NullColumnValueException;

    boolean hasAColumnReference();

    boolean isAColumnReference();

    String asString();

    void reset();
}
