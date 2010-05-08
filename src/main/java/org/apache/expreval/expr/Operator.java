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

package org.apache.expreval.expr;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

public enum Operator {

    PLUS("+", null, null),
    MINUS("-", null, null),
    MULT("*", null, null),
    DIV("/", null, null),
    MOD("%", null, null),
    NEGATIVE("-", null, null),

    EQ("==",
       CompareFilter.CompareOp.EQUAL,
       CompareFilter.CompareOp.EQUAL),
    GT("<",
       CompareFilter.CompareOp.GREATER,
       CompareFilter.CompareOp.LESS_OR_EQUAL),
    GTEQ(">=",
         CompareFilter.CompareOp.GREATER_OR_EQUAL,
         CompareFilter.CompareOp.LESS),
    LT("<",
       CompareFilter.CompareOp.LESS,
       CompareFilter.CompareOp.GREATER_OR_EQUAL),
    LTEQ("<=",
         CompareFilter.CompareOp.LESS_OR_EQUAL,
         CompareFilter.CompareOp.GREATER),
    NOTEQ("!=",
          CompareFilter.CompareOp.NOT_EQUAL,
          CompareFilter.CompareOp.NOT_EQUAL),

    AND("AND", null, null),
    OR("OR", null, null);

    final String opStr;
    final CompareFilter.CompareOp compareOpLeft;
    final CompareFilter.CompareOp compareOpRight;

    Operator(final String opStr,
             final CompareFilter.CompareOp compareOpLeft,
             final CompareFilter.CompareOp compareOpRight) {
        this.opStr = opStr;
        this.compareOpLeft = compareOpLeft;
        this.compareOpRight = compareOpRight;
    }

    public String toString() {
        return this.opStr;
    }

    public CompareFilter.CompareOp getCompareOpLeft() throws HBqlException {
        if (this.compareOpRight == null)
            throw new HBqlException("Invalid operator: " + this);
        return this.compareOpLeft;
    }

    public CompareFilter.CompareOp getCompareOpRight() throws HBqlException {
        if (this.compareOpRight == null)
            throw new HBqlException("Invalid operator: " + this);
        return this.compareOpRight;
    }
}