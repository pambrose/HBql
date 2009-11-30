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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.expreval.client.InternalErrorException;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.NavigableMap;

public class SelectFamilyAttrib extends ColumnAttrib {

    public SelectFamilyAttrib(final String familyName) throws HBqlException {
        super(familyName, "", "", null, false, null, null);
    }

    public boolean isASelectFamilyAttrib() {
        return true;
    }

    public String getNameToUseInExceptions() {
        return this.getFamilyQualifiedName();
    }

    public Map<Long, Object> getVersionMap(final Object recordObj) throws HBqlException {
        throw new InternalErrorException();
    }

    protected Class getComponentType() throws HBqlException {
        throw new InternalErrorException();
    }

    public Object getCurrentValue(final Object obj) throws HBqlException {
        throw new InternalErrorException();
    }

    protected Method getMethod(final String methodName, final Class<?>... params) throws NoSuchMethodException {
        return null;
    }

    public String getEnclosingClassName() {
        return null;
    }

    public void setCurrentValue(final Object obj, final long timestamp, final Object val) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setUnMappedCurrentValue(final Object obj,
                                        final String name,
                                        final byte[] value) throws HBqlException {
        throw new InternalErrorException();
    }

    public void setUnMappedVersionMap(final Object obj,
                                      final String name,
                                      final NavigableMap<Long, byte[]> timeStampMap) throws HBqlException {
        throw new InternalErrorException();
    }

    public void resetDefaultValue() {
        // No op
    }

    public Object getDefaultValue() {
        return null;
    }

    public boolean hasDefaultArg() {
        return false;
    }
}
