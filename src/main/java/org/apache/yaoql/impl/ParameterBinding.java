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

package org.apache.yaoql.impl;

import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

import java.util.Map;

public abstract class ParameterBinding {

    final Map<String, Object> parameterMap = Maps.newHashMap();

    public abstract String getQuery();

    public void setParameter(final String name, final Object val) {
        this.getParameterMap().put(name, val);
    }

    protected void applyParameters(final ExpressionTree expressionTree) throws HBqlException {
        for (final String key : this.getParameterMap().keySet()) {
            int cnt = expressionTree.setParameter(key, this.getParameterMap().get(key));
            if (cnt == 0)
                throw new HBqlException("Parameter name " + key + " does not exist in " + this.getQuery());
        }
    }

    private Map<String, Object> getParameterMap() {
        return parameterMap;
    }
}
