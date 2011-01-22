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

package org.apache.expreval.expr;

import org.apache.expreval.client.InternalErrorException;
import org.apache.expreval.expr.literal.BooleanLiteral;
import org.apache.expreval.expr.literal.ByteLiteral;
import org.apache.expreval.expr.literal.DateLiteral;
import org.apache.expreval.expr.literal.DoubleLiteral;
import org.apache.expreval.expr.literal.FloatLiteral;
import org.apache.expreval.expr.literal.IntegerLiteral;
import org.apache.expreval.expr.literal.LongLiteral;
import org.apache.expreval.expr.literal.ShortLiteral;
import org.apache.expreval.expr.literal.StringLiteral;
import org.apache.expreval.expr.node.BooleanValue;
import org.apache.expreval.expr.node.ByteValue;
import org.apache.expreval.expr.node.DateValue;
import org.apache.expreval.expr.node.DoubleValue;
import org.apache.expreval.expr.node.FloatValue;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.expreval.expr.node.IntegerValue;
import org.apache.expreval.expr.node.LongValue;
import org.apache.expreval.expr.node.NumberValue;
import org.apache.expreval.expr.node.ShortValue;
import org.apache.expreval.expr.node.StringValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.util.Lists;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public abstract class TypeSignature implements Serializable {

    private static final long serialVersionUID = 1L;

    private Class<? extends GenericValue> returnType;
    private List<Class<? extends GenericValue>> argsTypeSignature = Lists.newArrayList();

    private           Class       literalCastClass;
    private transient Constructor literalConstructor;

    public TypeSignature() {
    }

    public TypeSignature(final Class<? extends GenericValue> returnType,
                         final Class<? extends GenericValue>... argsTypeSignature) {
        this.returnType = returnType;
        this.argsTypeSignature.addAll(Arrays.asList(argsTypeSignature));

        this.literalCastClass = this.getLiteralCastClass(this.getReturnType());
        this.literalConstructor = this.getLiteralConstructor(this.getReturnType(), this.getLiteralCastClass());
    }

    public Class<? extends GenericValue> getReturnType() {
        return this.returnType;
    }

    public List<Class<? extends GenericValue>> getArgTypeList() {
        return this.argsTypeSignature;
    }

    private Constructor getLiteralConstructor() {
        return this.literalConstructor;
    }

    private Class getLiteralCastClass() {
        return this.literalCastClass;
    }

    public Class<? extends GenericValue> getArg(final int i) {
        return this.getArgTypeList().get(i);
    }

    public int getArgCount() {
        return this.getArgTypeList().size();
    }

    private Class getLiteralCastClass(final Class<? extends GenericValue> clazz) {

        if (clazz == null)
            return null;
        else if (clazz == BooleanValue.class)
            return Boolean.class;
        else if (clazz == ByteValue.class)
            return Byte.class;
        else if (clazz == ShortValue.class)
            return Short.class;
        else if (clazz == IntegerValue.class)
            return Integer.class;
        else if (clazz == LongValue.class)
            return Long.class;
        else if (clazz == FloatValue.class)
            return Float.class;
        else if (clazz == DoubleValue.class)
            return Double.class;
        else if (clazz == NumberValue.class)
            return Number.class;
        else if (clazz == StringValue.class)
            return String.class;
        else if (clazz == DateValue.class)
            return Long.class;    // Note this is Long and not Date
        else {
            throw new RuntimeException("Invalid return type in signature: " + clazz.getName());
        }
    }

    private Constructor getLiteralConstructor(final Class clazz, final Class aLiteralCastClass) {

        final Class retval;

        try {
            if (clazz == null)
                return null;
            else if (clazz == BooleanValue.class || clazz == Boolean.class)
                retval = BooleanLiteral.class;
            else if (NumericType.isAByte(clazz))
                retval = ByteLiteral.class;
            else if (NumericType.isAShort(clazz))
                retval = ShortLiteral.class;
            else if (NumericType.isAnInteger(clazz))
                retval = IntegerLiteral.class;
            else if (NumericType.isALong(clazz))
                retval = LongLiteral.class;
            else if (NumericType.isAFloat(clazz))
                retval = FloatLiteral.class;
            else if (NumericType.isADouble(clazz))
                retval = DoubleLiteral.class;
            else if (clazz == NumberValue.class || clazz == Number.class)
                return null;
            else if (clazz == StringValue.class || clazz == String.class)
                retval = StringLiteral.class;
            else if (clazz == DateValue.class || clazz == Date.class)
                retval = DateLiteral.class;
            else
                throw new RuntimeException("Invalid return type in signature: " + clazz.getName());

            return retval.getConstructor(aLiteralCastClass);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Invalid literal constructor in signature: " + clazz.getName());
        }
    }

    public GenericValue newLiteral(final Object val) throws HBqlException {
        try {

            final Constructor constructor;
            final Object castedObject;

            if (this.getReturnType() == NumberValue.class) {
                constructor = this.getLiteralConstructor(val.getClass(), val.getClass());
                castedObject = val;
            }
            else {
                constructor = this.getLiteralConstructor();
                castedObject = this.getLiteralCastClass().cast(val);
            }

            return (GenericValue)constructor.newInstance(castedObject);
        }
        catch (InstantiationException e) {
            throw new InternalErrorException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new InternalErrorException(e.getMessage());
        }
        catch (InvocationTargetException e) {
            throw new InternalErrorException(e.getMessage());
        }
    }
}
