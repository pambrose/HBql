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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.expr.ArgumentListTypeSignature;
import org.apache.expreval.expr.ExpressionProperty;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultArg extends ExpressionProperty implements Serializable {

    private static final long serialVersionUID = 1L;

    // We have to make value transient because Object is not serializable for RecordFilter
    // We will compute it again on the server after reset is called
    private transient Object value = null;
    private AtomicBoolean atomicComputed = new AtomicBoolean(false);

    public DefaultArg(final Class<? extends GenericValue> exprType, final GenericValue expr) throws HBqlException {
        super(new ArgumentListTypeSignature(exprType), expr);

        this.validate();

        // This will force the type checking to happen
        this.getDefaultValue();
    }

    private AtomicBoolean getAtomicComputed() {
        return this.atomicComputed;
    }

    public void reset() {
        this.getAtomicComputed().set(false);
        this.value = null;
    }

    public Object getDefaultValue() throws HBqlException {
        if (!this.getAtomicComputed().get()) {
            synchronized (this) {
                if (!this.getAtomicComputed().get()) {
                    // Type checking happens in this call, so we force it above in the constructor
                    this.value = this.evaluateConstant(0, false);

                    this.getAtomicComputed().set(true);
                }
            }
        }
        return this.value;
    }

    public String asString() {
        return this.getGenericValue(0).asString();
    }
}