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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.Utils;
import org.apache.hadoop.hbase.hbql.io.IO;
import org.apache.hadoop.hbase.hbql.parser.ParserSupport;
import org.apache.hadoop.hbase.hbql.statement.args.ColumnWidth;
import org.apache.hadoop.hbase.hbql.statement.args.DefaultArg;
import org.apache.hadoop.hbase.hbql.util.AtomicReferences;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ColumnAttrib implements Serializable {

    private static final long serialVersionUID = 1L;

    private ColumnDefinition columnDefinition;

    private           AtomicReference<byte[]> atomicFamilyQualifiedBytes = AtomicReferences.newAtomicReference();
    private           AtomicReference<byte[]> atomicFamilyBytes          = AtomicReferences.newAtomicReference();
    private           AtomicReference<byte[]> atomicColumnBytes          = AtomicReferences.newAtomicReference();
    private transient Method                  getterMethod               = null;
    private transient Method                  setterMethod               = null;
    private boolean embedded;

    public ColumnAttrib() {
    }

    protected ColumnAttrib(final ColumnDefinition columnDefinition) {
        this.columnDefinition = columnDefinition;
        this.embedded = this.getFamilyName() != null && this.getFamilyName().equals(ParserSupport.EMBEDDED);
    }

    private boolean isEmbedded() {
        return this.embedded;
    }

    public Object getDefaultValue() throws HBqlException {
        return (this.hasDefaultArg()) ? this.getDefaultArg().getDefaultValue() : null;
    }

    public boolean hasDefaultArg() throws HBqlException {
        return this.getDefaultArg() != null;
    }

    protected DefaultArg getDefaultArg() {
        return null;
    }

    public abstract Object getCurrentValue(final Object obj) throws HBqlException, ResultMissingColumnException, NullColumnValueException;

    public abstract void setCurrentValue(final Object obj,
                                         final long timestamp,
                                         final Object val) throws HBqlException;

    public abstract Map<Long, Object> getVersionMap(final Object obj) throws HBqlException;

    public abstract void setUnMappedCurrentValue(final Object obj,
                                                 final String name,
                                                 final byte[] value) throws HBqlException;

    public abstract void setUnMappedVersionMap(final Object obj,
                                               final String name,
                                               final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException;

    protected abstract Method getMethod(final String methodName,
                                        final Class<?>... params) throws NoSuchMethodException, HBqlException;

    protected abstract Class getComponentType() throws HBqlException;

    public abstract String getNameToUseInExceptions();

    public abstract String getEnclosingClassName();

    // This is necessary before sending off with filter
    public void resetDefaultValue() throws HBqlException {
        if (this.hasDefaultArg())
            this.getDefaultArg().reset();
    }

    public void setVersionMap(final Object obj, final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {

        final Map<Long, Object> mapVal = this.getVersionMap(obj);

        for (final Long timestamp : timeStampMap.keySet()) {
            final Object val = this.getValueFromBytes(obj, timeStampMap.get(timestamp));
            mapVal.put(timestamp, val);
        }
    }

    public String getFamilyQualifiedName() {
        if (!this.isEmbedded() && Utils.isValidString(this.getFamilyName()))
            return this.getFamilyName() + ":" + this.getColumnName();
        else
            return this.getColumnName();
    }

    private AtomicReference<byte[]> getAtomicFamilyQualifiedBytes() {
        return this.atomicFamilyQualifiedBytes;
    }

    public byte[] getFamilyQualifiedNameAsBytes() throws HBqlException {
        if (this.getAtomicFamilyQualifiedBytes().get() == null)
            synchronized (this) {
                if (this.getAtomicFamilyQualifiedBytes().get() == null) {
                    final byte[] b = IO.getSerialization().getStringAsBytes(this.getFamilyQualifiedName());
                    this.getAtomicFamilyQualifiedBytes().set(b);
                }
            }
        return this.getAtomicFamilyQualifiedBytes().get();
    }

    private AtomicReference<byte[]> getAtomicFamilyBytes() {
        return this.atomicFamilyBytes;
    }

    public byte[] getFamilyNameAsBytes() throws HBqlException {
        if (this.getAtomicFamilyBytes().get() == null)
            synchronized (this) {
                if (this.getAtomicFamilyBytes().get() == null) {
                    final byte[] b = IO.getSerialization().getStringAsBytes(this.getFamilyName());
                    this.getAtomicFamilyBytes().set(b);
                }
            }
        return this.getAtomicFamilyBytes().get();
    }

    private AtomicReference<byte[]> getAtomicColumnBytes() {
        return this.atomicColumnBytes;
    }

    public byte[] getColumnNameAsBytes() throws HBqlException {
        if (this.getAtomicColumnBytes().get() == null)
            synchronized (this) {
                if (this.getAtomicColumnBytes().get() == null) {
                    final byte[] b = IO.getSerialization().getStringAsBytes(this.getColumnName());
                    this.getAtomicColumnBytes().set(b);
                }
            }
        return this.getAtomicColumnBytes().get();
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
            if (Utils.isValidString(this.getGetter())) {
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
            if (Utils.isValidString(this.getSetter())) {
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

        final byte[] retval;

        if (this.hasGetter()) {
            retval = this.invokeGetterMethod(obj);
        }
        else {
            final Object value;
            try {
                value = this.getCurrentValue(obj);
            }
            catch (ResultMissingColumnException e) {
                return null;
            }
            catch (NullColumnValueException e) {
                return null;
            }

            if (this.isAnArray()) {
                retval = IO.getSerialization().getArrayAsBytes(this.getFieldType(), value);
            }
            else {
                this.validateValueWidth(value);
                retval = IO.getSerialization().getScalarAsBytes(this.getFieldType(), value);
            }
        }

        return retval;
    }

    public void validateValueWidth(final Object value) throws HBqlException {

        final ColumnWidth columnWidth = this.getColumnDefinition().getColumnWidth();
        if (columnWidth.isWidthSpecified()) {
            final int width = columnWidth.getWidth();
            if (width > 0 && value instanceof String) {
                final String str = (String)value;
                if (str.length() != width)
                    throw new HBqlException("Invalid length in " + this.getNameToUseInExceptions()
                                            + " expecting width " + width + " but found " + str.length()
                                            + " with string \"" + str + "\"");
            }
        }
    }

    public Object getValueFromBytes(final Object obj, final byte[] b) throws HBqlException {

        if (this.hasSetter())
            return this.invokeSetterMethod(obj, b);

        if (this.isAnArray())
            return IO.getSerialization().getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
        else
            return IO.getSerialization().getScalarFromBytes(this.getFieldType(), b);
    }

    public Object getValueFromBytes(final Result result) throws HBqlException, ResultMissingColumnException, NullColumnValueException {

        if (this.isAKeyAttrib()) {
            return IO.getSerialization().getStringFromBytes(result.getRow());
        }
        else {
            if (!result.containsColumn(this.getFamilyNameAsBytes(), this.getColumnNameAsBytes())) {

                // See if a default value is present
                if (!this.hasDefaultArg())
                    throw new ResultMissingColumnException(this.getFamilyQualifiedName());

                return this.getDefaultValue();
            }

            final byte[] b = result.getValue(this.getFamilyNameAsBytes(), this.getColumnNameAsBytes());

            if (b == null) {
                throw new NullColumnValueException(this.getFamilyQualifiedName());
            }
            else {
                if (this.isAnArray())
                    return IO.getSerialization().getArrayFromBytes(this.getFieldType(), this.getComponentType(), b);
                else
                    return IO.getSerialization().getScalarFromBytes(this.getFieldType(), b);
            }
        }
    }

    public void setCurrentValue(final Object obj, final long timestamp, final byte[] b) throws HBqlException {
        final Object val = this.getValueFromBytes(obj, b);
        this.setCurrentValue(obj, timestamp, val);
    }

    protected String getGetter() {
        return this.getColumnDefinition().getGetter();
    }

    protected String getSetter() {
        return this.getColumnDefinition().getSetter();
    }

    protected Method getGetterMethod() {
        return this.getterMethod;
    }

    protected Method getSetterMethod() {
        return this.setterMethod;
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

    public ColumnDefinition getColumnDefinition() {
        return this.columnDefinition;
    }

    public boolean hasAlias() {
        return Utils.isValidString(this.getColumnDefinition().getAliasName());
    }

    public String getAliasName() {
        return (this.hasAlias()) ? this.getColumnDefinition().getAliasName() : this.getFamilyQualifiedName();
    }

    public boolean isASelectFamilyAttrib() {
        return false;
    }

    public boolean isAnArray() {
        return this.getColumnDefinition().isAnArray();
    }

    public String getFamilyName() {
        return this.getColumnDefinition().getFamilyName();
    }

    public String getColumnName() {
        return this.getColumnDefinition().getColumnName();
    }

    public FieldType getFieldType() {
        return this.getColumnDefinition().getFieldType();
    }

    public boolean isAKeyAttrib() {
        return false;
    }
}
