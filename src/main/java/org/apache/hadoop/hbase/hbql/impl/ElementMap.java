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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;

import java.io.Serializable;
import java.util.Map;

public class ElementMap<T extends Serializable> implements Serializable {

    private final Map<String, T> map = Maps.newHashMap();

    private final RecordImpl record;

    public ElementMap(final RecordImpl record) {
        this.record = record;
    }

    private RecordImpl getRecord() {
        return this.record;
    }

    public Map<String, T> getMap() {
        return map;
    }

    private String getNameToUse(final String name) {
        final ColumnAttrib attrib = this.getRecord().getSchema().getAttribByVariableName(name);
        if (attrib == null)
            return name;
        else
            return attrib.getFamilyQualifiedName();
    }

    public void addElement(final String name, final T value) {
        this.getMap().put(this.getNameToUse(name), value);
    }

    public boolean containsName(final String name) {
        return this.getMap().containsKey(name);
    }

    private T getElement(final String name) {
        return this.getMap().get(name);
    }

    public T findElement(final String name) {

        // First try the name given.
        // If that doesn't work, then try qualified name
        if (this.containsName(name))
            return this.getElement(name);

        // Look up by  alias name
        final ColumnAttrib attrib = this.getRecord().getSchema().getAttribByVariableName(name);

        if (attrib != null) {
            final String qualifiedName = attrib.getFamilyQualifiedName();
            if (!qualifiedName.equals(name) && this.containsName(qualifiedName))
                return this.getElement(qualifiedName);
        }

        return null;
    }

    public void clear() {

        this.getMap().clear();
    }
}
