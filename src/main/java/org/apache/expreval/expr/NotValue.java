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

import org.apache.expreval.expr.node.GenericValue;

import java.util.List;

public abstract class NotValue<T extends GenericExpression> extends DelegateStmt<T> {

    private final boolean not;

    protected NotValue(final ExpressionType type, final boolean not, GenericValue... args) {
        super(type, args);
        this.not = not;
    }

    protected NotValue(final ExpressionType type, final boolean not, final List<GenericValue> args) {
        super(type, args);
        this.not = not;
    }

    protected NotValue(final ExpressionType type, final boolean not, final GenericValue arg, final List<GenericValue> argList) {
        super(type, arg, argList);
        this.not = not;
    }

    public boolean isNot() {
        return this.not;
    }

    protected String notAsString() {
        return (this.isNot()) ? " NOT" : "";
    }
}