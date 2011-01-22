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

package org.apache.expreval.expr.var;

import org.apache.expreval.client.NullColumnValueException;
import org.apache.expreval.client.ResultMissingColumnException;
import org.apache.expreval.expr.node.DateValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;

import java.util.Date;

public class DateColumn extends GenericColumn<DateValue> implements DateValue {

    public DateColumn(final ColumnAttrib attrib) {
        super(attrib);
    }

    public Long getValue(final HConnectionImpl conn, final Object object) throws HBqlException,
                                                                                 ResultMissingColumnException,
                                                                                 NullColumnValueException {
        final Date val;

        if (this.getExpressionContext().useResultData())
            val = (Date)this.getColumnAttrib().getValueFromBytes((Result)object);
        else
            val = (Date)this.getColumnAttrib().getCurrentValue(object);

        return val.getTime();
    }
}