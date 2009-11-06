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

package org.apache.hadoop.hbase.contrib.hbql.statement.args;

import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.TypeSignature;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.StringValue;

public abstract class SelectArgs extends MultipleExpressionContext {

    public static enum Type {

        NOARGSKEY(new TypeSignature(null)),
        SINGLEKEY(new TypeSignature(null, StringValue.class)),
        KEYRANGE(new TypeSignature(null, StringValue.class, StringValue.class)),
        TIMESTAMPRANGE(new TypeSignature(null, DateValue.class, DateValue.class)),
        LIMIT(new TypeSignature(null, LongValue.class)),
        VERSION(new TypeSignature(null, LongValue.class));

        private final TypeSignature typeSignature;

        Type(final TypeSignature typeSignature) {
            this.typeSignature = typeSignature;
        }

        public TypeSignature getTypeSignature() {
            return typeSignature;
        }
    }

    protected SelectArgs(final Type type, final GenericValue... exprs) {
        super(type.getTypeSignature(), exprs);
    }

    public boolean useResultData() {
        return false;
    }
}
