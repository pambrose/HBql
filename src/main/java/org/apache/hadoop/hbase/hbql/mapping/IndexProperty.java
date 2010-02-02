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

package org.apache.hadoop.hbase.hbql.mapping;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class IndexProperty extends FamilyProperty {

    final String column;
    final String type;

    public IndexProperty(final String text, final String column, final String type) {
        super(text);
        this.column = column;
        this.type = type;
    }

    public ColumnDefinition getColumnDefinition() throws HBqlException {
        final ColumnDefinition colDef = ColumnDefinition.newIndexedColumn(this.column, this.type);
        if (colDef.getFieldType() == null || colDef.getFieldType().getIndexType() == null)
            throw new HBqlException("Invalid index type: " + this.column + " " + this.type);
        return colDef;
    }
}