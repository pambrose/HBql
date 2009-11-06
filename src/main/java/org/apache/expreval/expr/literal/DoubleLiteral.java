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

package org.apache.expreval.expr.literal;

import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.GenericValue;

public class DoubleLiteral extends GenericLiteral<Double> implements DoubleValue {

    public static GenericValue valueOf(final String value) {
        final String upper = (value != null) ? value.toUpperCase() : "0";
        return upper.endsWith("F") ? new FloatLiteral(upper) : new DoubleLiteral(upper);
    }

    public DoubleLiteral(final String value) {
        super(Double.valueOf(value.endsWith("D") ? value.substring(0, value.length() - 1) : value));
    }

    public DoubleLiteral(final Double value) {
        super(value);
    }

    protected Class<? extends GenericValue> getReturnType() {
        return DoubleValue.class;
    }
}