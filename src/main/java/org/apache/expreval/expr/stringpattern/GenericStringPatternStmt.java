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

package org.apache.expreval.expr.stringpattern;

import org.apache.expreval.expr.ExpressionType;
import org.apache.expreval.expr.NotValue;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.GenericValue;

public abstract class GenericStringPatternStmt extends NotValue<GenericStringPatternStmt> implements BooleanValue {

    protected GenericStringPatternStmt(final GenericValue arg0, final boolean not, final GenericValue arg1) {
        super(ExpressionType.STRINGPATTERN, not, arg0, arg1);
    }

    protected abstract String getFunctionName();

    public String asString() {
        return this.getExprArg(0).asString() + notAsString() + " "
               + this.getFunctionName() + " " + this.getExprArg(1).asString();
    }
}