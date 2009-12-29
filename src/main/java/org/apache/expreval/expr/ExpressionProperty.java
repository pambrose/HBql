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

package org.apache.expreval.expr;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

public abstract class ExpressionProperty extends MultipleExpressionContext {

    private final PropertyType propertyType;

    public ExpressionProperty(final PropertyType propertyType, final GenericValue... exprs) {
        super(propertyType.getTypeSignature(), exprs);
        this.propertyType = propertyType;
    }

    // This is for DefaultArg
    public ExpressionProperty(final ArgumentListTypeSignature argumentListTypeSignature, final GenericValue expr) {
        super(argumentListTypeSignature, expr);
        this.propertyType = null;
    }

    public PropertyType getPropertyType() {
        return this.propertyType;
    }

    public boolean useResultData() {
        return false;
    }

    public boolean allowColumns() {
        return false;
    }

    public void validate() throws HBqlException {
        this.validateTypes(this.allowColumns(), false);
    }
}
