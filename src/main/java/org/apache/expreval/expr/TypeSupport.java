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
import org.apache.expreval.expr.node.CharValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ObjectValue;
import org.apache.expreval.expr.node.StringValue;

import java.util.Collection;

public class TypeSupport {

    public static boolean isACollection(final Object obj) {
        return isParentClass(Collection.class, obj.getClass());
    }

    public static boolean isParentClass(final Class parentClass, final Class... childrenClasses) {

        final boolean parentIsANumber = NumericType.isANumber(parentClass);

        for (final Class clazz : childrenClasses) {

            if (clazz == null)
                continue;

            if (parentIsANumber && NumericType.isANumber(clazz)) {
                if (!NumericType.isAssignable(parentClass, clazz))
                    return false;
            }
            else {
                if (!parentClass.isAssignableFrom(clazz))
                    return false;
            }
        }
        return true;
    }

    public static Class<? extends GenericValue> getGenericExprType(final GenericValue val) {

        final Class clazz = val.getClass();

        if (isParentClass(BooleanValue.class, clazz))
            return BooleanValue.class;

        if (isParentClass(ByteValue.class, clazz))
            return ByteValue.class;

        if (isParentClass(CharValue.class, clazz))
            return CharValue.class;

        if (isParentClass(NumberValue.class, clazz))
            return NumberValue.class;

        if (isParentClass(StringValue.class, clazz))
            return StringValue.class;

        if (isParentClass(DateValue.class, clazz))
            return DateValue.class;

        if (isParentClass(ObjectValue.class, clazz))
            return ObjectValue.class;

        return null;
    }

    public static boolean allowsNullValues(final Class clazz) {

        if (isParentClass(StringValue.class, clazz))
            return true;

        if (isParentClass(ObjectValue.class, clazz))
            return true;

        return false;
    }
}
