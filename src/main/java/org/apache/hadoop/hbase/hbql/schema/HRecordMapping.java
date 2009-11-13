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

package org.apache.hadoop.hbase.hbql.schema;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.hbql.statement.SchemaContext;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;

import java.util.List;

public class HRecordMapping extends Mapping {


    public HRecordMapping(final SchemaContext schemaContext) {
        super(schemaContext);
    }

    public Object newObject(final SchemaContext schemaContext,
                            final List<SelectElement> selectElementList,
                            final int maxVersions,
                            final Result result) throws HBqlException {

        // Create object and assign values
        final RecordImpl newrec = new RecordImpl(schemaContext);
        this.assignSelectValues(newrec, selectElementList, maxVersions, result);
        return newrec;
    }

    private void assignSelectValues(final Object newobj,
                                    final List<SelectElement> selectElementList,
                                    final int maxVersions,
                                    final Result result) throws HBqlException {

        // Set key value
        this.getSchema().getKeyAttrib().setCurrentValue(newobj, 0, result.getRow());

        // Set the non-key values
        for (final SelectElement selectElement : selectElementList)
            selectElement.assignSelectValue(newobj, maxVersions, result);
    }

    public ColumnAttrib getKeyAttrib() throws HBqlException {
        return this.getSchema().getKeyAttrib();
    }

    public ColumnAttrib getAttribFromFamilyQualifiedName(final String familyName,
                                                         final String columnName) throws HBqlException {
        return this.getHBaseSchema().getAttribFromFamilyQualifiedName(familyName + ":" + columnName);
    }

    public ColumnAttrib getAttribByVariableName(final String name) throws HBqlException {
        return this.getSchema().getAttribByVariableName(name);
    }
}
