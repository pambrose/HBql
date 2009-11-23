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

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;

import java.io.Serializable;
import java.util.HashMap;

public class ElementMap<T extends Value> extends HashMap<String, T> implements Serializable {

    private final HRecordImpl record;

    public ElementMap(final HRecordImpl record) {
        this.record = record;
    }

    private HRecordImpl getRecord() {
        return this.record;
    }

    public void addElement(final T value) throws HBqlException {
        final ColumnAttrib attrib = this.getRecord().getResultAccessor().getColumnAttribByName(value.getName());
        final String name = (attrib == null) ? value.getName() : attrib.getFamilyQualifiedName();
        this.put(name, value);
    }

    public boolean containsName(final String name) {
        return this.containsKey(name);
    }

    private T getElement(final String name) {
        return this.get(name);
    }

    public T findElement(final String name) throws HBqlException {

        // First try the name given.
        // If that doesn't work, then try qualified name
        if (this.containsName(name))
            return this.getElement(name);

        // Look up by  alias name
        final ColumnAttrib attrib = this.getRecord().getResultAccessor().getColumnAttribByName(name);

        if (attrib != null) {
            final String qualifiedName = attrib.getFamilyQualifiedName();
            if (!qualifiedName.equals(name) && this.containsName(qualifiedName))
                return this.getElement(qualifiedName);
        }

        return null;
    }
}
