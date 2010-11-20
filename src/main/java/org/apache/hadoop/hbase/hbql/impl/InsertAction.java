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

package org.apache.hadoop.hbase.hbql.impl;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.IOException;

public class InsertAction implements BatchAction {

    private final Put actionValue;

    public InsertAction(final Put actionValue) {
        this.actionValue = actionValue;
    }

    private Put getActionValue() {
        return this.actionValue;
    }

    public void apply(final HTableInterface table) throws HBqlException {
        try {
            table.put(this.getActionValue());
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public String toString() {
        return "INSERT";
    }
}
