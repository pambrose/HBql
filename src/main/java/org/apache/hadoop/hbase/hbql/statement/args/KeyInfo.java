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

package org.apache.hadoop.hbase.hbql.statement.args;

import org.apache.expreval.expr.MultipleExpressionContext;
import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;

public class KeyInfo extends MultipleExpressionContext implements Serializable {

    final String keyName;
    final boolean keyWidthSpecified;

    int keyWidth = -1;

    public KeyInfo(final String keyName, final GenericValue val) {
        super(SelectStatementArgs.Type.WIDTH.getTypeSignature(), val);
        this.keyWidthSpecified = val != null;
        this.keyName = keyName;
    }

    public String getKeyName() {
        return this.keyName;
    }

    public boolean isKeyWidthSpecified() {
        return this.keyWidthSpecified;
    }

    public int getKeyWidth() {
        return this.keyWidth;
    }

    public String asString() {
        return this.getKeyName() + " KEY "
               + (this.isKeyWidthSpecified() ? "WIDTH " + this.getGenericValue(0).asString() : "");
    }

    public boolean useResultData() {
        return false;
    }

    public boolean allowColumns() {
        return false;
    }

    public void validate() throws HBqlException {
        if (this.isKeyWidthSpecified()) {
            this.keyWidth = ((Number)this.evaluateConstant(null, 0, false, null)).intValue();
            if (this.getKeyWidth() <= 0)
                throw new HBqlException("Invalid key width: " + this.getKeyWidth() + " for key " + this.getKeyName());
        }
    }
}