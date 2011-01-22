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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.expr.ArgumentListTypeSignature;
import org.apache.expreval.expr.ExpressionProperty;
import org.apache.expreval.expr.PropertyType;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;
import org.apache.expreval.expr.node.StringValue;

public abstract class SelectStatementArgs extends ExpressionProperty {

    public static enum ArgType implements PropertyType {

        NOARGSKEY(new ArgumentListTypeSignature(), ""),
        SINGLEKEY(new ArgumentListTypeSignature(StringValue.class), ""),
        KEYRANGE(new ArgumentListTypeSignature(StringValue.class, StringValue.class), ""),
        TIMESTAMPRANGE(new ArgumentListTypeSignature(DateValue.class, DateValue.class), "TIMESTAMP"),
        LIMIT(new ArgumentListTypeSignature(IntegerValue.class), "LIMIT"),
        SCANNERCACHE(new ArgumentListTypeSignature(IntegerValue.class), "SCANNER_CACHE"),
        VERSION(new ArgumentListTypeSignature(IntegerValue.class), "VERSION"),
        WIDTH(new ArgumentListTypeSignature(IntegerValue.class), "WIDTH");

        private final ArgumentListTypeSignature typeSignature;
        private final String                    description;

        ArgType(final ArgumentListTypeSignature typeSignature, final String description) {
            this.typeSignature = typeSignature;
            this.description = description;
        }

        public ArgumentListTypeSignature getTypeSignature() {
            return typeSignature;
        }

        public String getDescription() {
            return this.description;
        }
    }

    protected SelectStatementArgs(final ArgType argType, final GenericValue... exprs) {
        super(argType, exprs);
    }
}
