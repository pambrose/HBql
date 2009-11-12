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

import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.schema.HRecordMapping;
import org.apache.hadoop.hbase.hbql.schema.Mapping;
import org.apache.hadoop.hbase.hbql.statement.SelectStatement;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;

import java.util.List;

public class AggregateRecord extends RecordImpl {

    final List<SelectElement> selectElementList;

    private AggregateRecord(final HRecordMapping mapping,
                            final List<SelectElement> selectElementList) throws HBqlException {
        super(mapping);

        this.selectElementList = selectElementList;

        // Set key value
        this.getSchema().getKeyAttrib().setCurrentValue(this, 0, "");

        for (final SelectElement selectElement : this.getSelectElementList()) {
            final AggregateValue val = selectElement.newAggregateValue();
            val.initAggregateValue();
            this.addElement(val);
        }
    }

    public static AggregateRecord newAggregateRecord(final Mapping mapping,
                                                     final SelectStatement selectStmt) throws HBqlException {

        if (!(mapping instanceof HRecordMapping))
            throw new HBqlException("Aggregates do not work with Annotated objects yet");

        if (selectStmt.isAnAggregateQuery())
            return new AggregateRecord((HRecordMapping)mapping, selectStmt.getSelectElementList());
        else
            return null;
    }

    private List<SelectElement> getSelectElementList() {
        return this.selectElementList;
    }

    public void applyValues(final Result result) throws HBqlException {

        for (final ColumnValue val : this.getColumnValuesMap().values()) {
            if (val instanceof AggregateValue) {
                final AggregateValue aggregateValue = (AggregateValue)val;
                try {
                    aggregateValue.applyValues(result);
                }
                catch (ResultMissingColumnException e) {
                    // no op
                }
            }
        }
    }
}
