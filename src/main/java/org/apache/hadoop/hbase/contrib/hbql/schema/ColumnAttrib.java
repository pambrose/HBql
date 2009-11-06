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

package org.apache.hadoop.hbase.contrib.hbql.schema;

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.io.IO;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

public abstract class ColumnAttrib implements Serializable {

    private final String familyName;
    private final String columnName;
    private final String aliasName;
    private final FieldType fieldType;
    private volatile byte[] familyBytes = null;
    private volatile byte[] columnBytes = null;
    private final String getter;
    private final String setter;
    private final boolean familyDefault;
    private final boolean anArray;
    private transient Method getterMethod = null;
    private transient Method setterMethod = null;

    protected ColumnAttrib(final String familyName,
                           final String columnName,
                           final String aliasName,
                           final boolean familyDefault,
                           final FieldType fieldType,
                           final boolean isArray,
                           final String getter,
                           final String setter,
                           final GenericValue defaultValueExpr) throws HBqlException {

        this.familyName = familyName;
        this.columnName = columnName;
        this.aliasName = aliasName;
        this.familyDefault = familyDefault;
        this.fieldType = fieldType;
        this.anArray = isArray;
        this.getter = getter;
        this.setter = setter;
    }

    public abstract String asString();

    // This is necessary before sending off with filter
    public abstract void resetDefaultValue();

    public abstract Object getDefaultValue() throws HBqlException;

    public abstract boolean hasDefaultArg();

    public abstract Object getCurrentValue(final Object obj) throws HBqlException;

    public abstract void setCurrentValue(final Object obj,
                                         final long timestamp,
                                         final Object val) throws HBqlException;

    public abstract Map<Long, Object> getVersionMap(final Object obj) throws HBqlException;

    public abstract void setFamilyDefaultCurrentValue(final Object obj,
                                                      final String name,
                                                      final byte[] value) throws HBqlException;

    public abstract void setFamilyDefaultVersionMap(final Object obj,
                                                    final String name,
                                                    final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException;

    public void setVersionMap(final Object obj, final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {

        final Map<Long, Object> mapVal = this.getVersionMap(obj);

        for (final Long timestamp : timeStampMap.keySet()) {
            final Object val = this.getValueFromBytes(obj, timeStampMap.get(timestamp));
            mapVal.put(timestamp, val);
        }
    }

    protected abstract Method getMethod(final String methodName,
                                        final Class<?>... params) throws NoSuchMethodException, HBqlException;

    protected abstract Class getComponentType() throws HBqlException;

    public abstract String getNameToUseInExceptions();

    public abstract String getEnclosingClassName();

    public String getFamilyQualifiedName() {
        if (this.getFamilyName() != null && this.getFamilyName().length() > 0)
            return this.getFamilyName() + ":" + this.getColumnName();
        else
            return this.getColumnName();
    }

    public byte[] getFamilyNameBytes() throws HBqlException {
        if (this.familyBytes == null)
            synchronized (this) {
                if (this.familyBytes == null)
                    this.familyBytes = IO.getSerialization().getStringAsBytes(this.getFamilyName());
            }
        return this.familyBytes;
    }

    public byte[] getColumnNameBytes() throws HBqlException {
        if (this.columnBytes == null)
            synchronized (this) {
                if (this.columnBytes == null)
                    this.columnBytes = IO.getSerialization().getStringAsBytes(this.getColumnName());
            }
        return this.columnBytes;
    }

    public boolean equals(final Object o) {

        if (!(o instanceof ColumnAttrib))
            return false;

        final ColumnAttrib var = (ColumnAttrib)o;

        return var.getAliasName().equals(this.getAliasName())
               && var.getFamilyQualifiedName().equals(this.getFamilyQualifiedName());
    }

    public int hashCode() {
        return this.getAliasName().hashCode() + this.getFamilyQualifiedName().hashCode();
    }

    protected void defineAccessors() throws HBqlException {
        try {
            if (this.getGetter() != null && this.getGetter().length() > 0) {
                this.getterMethod = this.getMethod(this.getGetter());

                // Check return type of getter
                final Class<?> returnType = this.getGetterMethod().getReturnType();

                if (!(returnType.isArray() && returnType.getComponentType() == Byte.TYPE))
                    throw new HBqlException(this.getEnclosingClassName() + "." + this.getGetter() + "()"
                                            + " does not have a return type of byte[]");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HBqlException("Missing method byte[] " + this.getEnclosingClassName() + "."
                                    + this.getGetter() + "()");
        }

        try {
            if (this.getSetter() != null && this.getSetter().length() > 0) {
                this.setterMethod = this.getMethod(this.getSetter(), Class.forName("[B"));

                // Check if it takes single byte[] arg
                final Class<?>[] args = this.getSetterMethod().getParameterTypes();
                if (args.length != 1 || !(args[0].isArray() && args[0].getComponentType() == Byte.TYPE))
                    throw new HBqlException(this.getEnclosingClassName() + "." + this.getSetter() + "()"
                                            + " does not have single byte[] arg");
            }
        }
        catch (NoSuchMethodException e) {
            throw new HBqlException("Missing method " + this.getEnclosingClassName()
                                    + "." + this.getSetter() + "(byte[] arg)");
        }
        catch (ClassNotFoundException e) {
            // This will not be hit
            throw new HBqlException("Missing method " + this.getEnclosingClassName()
                                    + "." + this.getSetter() + "(byte[] arg)");
        }
    }

    public byte[] invokeGetterMethod(final Object obj) throws HBqlException {
        try {
            return (byte[])this.getGetterMethod().invoke(obj);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error getting value of " + this.getNameToUseInExceptions());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Error getting value of " + this.getNameToUseInExceptions());
        }
    }

    public Object invokeSetterMethod(final Object obj, final byte[] b) throws HBqlException {
        try {
            // TODO Resolve passing primitive to Object varargs
            return this.getSetterMethod().invoke(obj, b);
        }
        catch (IllegalAccessException e) {
            throw new HBqlException("Error setting value of " + this.getNameToUseInExceptions());
        }
        catch (InvocationTargetException e) {
            throw new HBqlException("Error setting value of " + this.getNameToUseInExceptions());
        }
    }

    public byte[] getValueAsBytes(final Object obj) throws HBqlException {

        if (this.hasGetter())
            return this.invokeGetterMethod(obj);

        final Object value = this.getCurrentValue(obj);

        if (this.isAnArray())
            return IO.getSerialization().getArrayAsBytes(this.getFieldType(), value);
        else
            return IO.getSerialization().getScalarAsBytes(this.getFieldType(), value);
    }

    public Object getValueFromBytes(final Object obj, final byte[] b) throws HBqlException {

        if (this.hasSetter())
            return this.invokeSetterMethod(obj, b);

        if (this.isAnArray())
            return IO.getSerialization().getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
        else
            return IO.getSerialization().getScalarFromBytes(this.getFieldType(), b);
    }

    public Object getValueFromBytes(final Result result) throws HBqlException, ResultMissingColumnException {

        if (this.isAKeyAttrib()) {
            return IO.getSerialization().getStringFromBytes(result.getRow());
        }
        else {

            if (!result.containsColumn(this.getFamilyNameBytes(), this.getColumnNameBytes())) {

                // See if a default value is present
                if (!this.hasDefaultArg())
                    throw new ResultMissingColumnException(this.getFamilyQualifiedName());

                return this.getDefaultValue();
            }

            final byte[] b = result.getValue(this.getFamilyNameBytes(), this.getColumnNameBytes());

            if (this.isAnArray())
                return IO.getSerialization().getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
            else
                return IO.getSerialization().getScalarFromBytes(this.getFieldType(), b);
        }
    }

    public void setCurrentValue(final Object obj, final long timestamp, final byte[] b) throws HBqlException {
        final Object val = this.getValueFromBytes(obj, b);
        this.setCurrentValue(obj, timestamp, val);
    }

    public byte[] getFamilyNameAsBytes() throws HBqlException {
        return IO.getSerialization().getStringAsBytes(this.getFamilyName());
    }

    public byte[] getColumnNameAsBytes() throws HBqlException {
        return IO.getSerialization().getStringAsBytes(this.getColumnName());
    }

    protected String getGetter() {
        return this.getter;
    }

    protected String getSetter() {
        return this.setter;
    }

    protected Method getGetterMethod() {
        return this.getterMethod;
    }

    protected Method getSetterMethod() {
        return this.setterMethod;
    }

    public boolean isFamilyDefaultAttrib() {
        return this.familyDefault;
    }

    protected boolean hasGetter() {
        return this.getGetterMethod() != null;
    }

    protected boolean hasSetter() {
        return this.getSetterMethod() != null;
    }

    public boolean isACurrentValue() {
        return true;
    }

    public boolean isAVersionValue() {
        return false;
    }

    public boolean hasAlias() {
        return this.aliasName != null && this.aliasName.length() > 0;
    }

    public String getAliasName() {
        return (this.hasAlias()) ? this.aliasName : this.getFamilyQualifiedName();
    }

    public boolean isASelectFamilyAttrib() {
        return false;
    }

    public boolean isAnArray() {
        return this.anArray;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public FieldType getFieldType() {
        return this.fieldType;
    }

    public boolean isAKeyAttrib() {
        return false;
    }
}
