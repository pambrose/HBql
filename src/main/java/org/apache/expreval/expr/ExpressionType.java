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

import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.ByteValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.StringValue;

public enum ExpressionType {

    BOOLEANCASE(new FunctionTypeSignature(BooleanValue.class)),
    STRINGCASE(new FunctionTypeSignature(StringValue.class)),
    DATECASE(new FunctionTypeSignature(DateValue.class)),
    NUMBERCASE(new FunctionTypeSignature(NumberValue.class)),

    BOOLEANCASEWHEN(new FunctionTypeSignature(BooleanValue.class, BooleanValue.class, BooleanValue.class)),
    STRINGCASEWHEN(new FunctionTypeSignature(StringValue.class, BooleanValue.class, StringValue.class)),
    DATECASEWHEN(new FunctionTypeSignature(DateValue.class, BooleanValue.class, DateValue.class)),
    NUMBERCASEWHEN(new FunctionTypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class)),

    BOOLEANCASEELSE(new FunctionTypeSignature(BooleanValue.class, BooleanValue.class)),
    STRINGCASEELSE(new FunctionTypeSignature(StringValue.class, StringValue.class)),
    DATECASEELSE(new FunctionTypeSignature(DateValue.class, BooleanValue.class, DateValue.class)),
    NUMBERCASEELSE(new FunctionTypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class)),

    BOOLEANIFTHEN(new FunctionTypeSignature(BooleanValue.class, BooleanValue.class, BooleanValue.class, BooleanValue.class)),
    BYTEIFTHEN(new FunctionTypeSignature(ByteValue.class, BooleanValue.class, ByteValue.class, ByteValue.class)),
    STRINGIFTHEN(new FunctionTypeSignature(StringValue.class, BooleanValue.class, StringValue.class, StringValue.class)),
    DATEIFTHEN(new FunctionTypeSignature(DateValue.class, BooleanValue.class, DateValue.class, DateValue.class)),
    NUMBERIFTHEN(new FunctionTypeSignature(NumberValue.class, BooleanValue.class, NumberValue.class, NumberValue.class)),

    STRINGCALCULATION(new FunctionTypeSignature(StringValue.class, StringValue.class, StringValue.class)),
    DATECALCULATION(new FunctionTypeSignature(DateValue.class, DateValue.class, DateValue.class)),
    NUMBERCALCULATION(new FunctionTypeSignature(NumberValue.class, NumberValue.class, NumberValue.class)),

    BYTEBETWEEN(new FunctionTypeSignature(BooleanValue.class, ByteValue.class, ByteValue.class, ByteValue.class)),
    STRINGBETWEEN(new FunctionTypeSignature(BooleanValue.class, StringValue.class, StringValue.class, StringValue.class)),
    DATEBETWEEN(new FunctionTypeSignature(BooleanValue.class, DateValue.class, DateValue.class, DateValue.class)),
    NUMBERBETWEEN(new FunctionTypeSignature(BooleanValue.class, NumberValue.class, NumberValue.class, NumberValue.class)),

    BOOLEANNULL(new FunctionTypeSignature(BooleanValue.class, BooleanValue.class)),
    BYTENULL(new FunctionTypeSignature(BooleanValue.class, ByteValue.class)),
    STRINGNULL(new FunctionTypeSignature(BooleanValue.class, StringValue.class)),
    DATENULL(new FunctionTypeSignature(BooleanValue.class, DateValue.class)),
    NUMBERNULL(new FunctionTypeSignature(BooleanValue.class, NumberValue.class)),

    STRINGPATTERN(new FunctionTypeSignature(BooleanValue.class, StringValue.class, StringValue.class)),

    // Args are left unspecified for IN Stmt
    INSTMT(new FunctionTypeSignature(BooleanValue.class));

    private final FunctionTypeSignature typeSignature;

    ExpressionType(final FunctionTypeSignature typeSignature) {
        this.typeSignature = typeSignature;
    }

    public FunctionTypeSignature getTypeSignature() {
        return typeSignature;
    }
}
