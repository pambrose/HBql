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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.expreval.expr.var.NamedParameter;
import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.hbql.client.HBqlException;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class NamedParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    private volatile List<NamedParameter> paramList = null;
    private final SortedSet<NamedParameter> paramSet = new TreeSet<NamedParameter>(NamedParameter.getComparator());

    private List<NamedParameter> getParamList() {
        return this.paramList;
    }

    private SortedSet<NamedParameter> getParamSet() {
        return this.paramSet;
    }

    public void addParameters(final Collection<NamedParameter> params) {
        if (params != null)
            this.getParamSet().addAll(params);
    }

    public List<NamedParameter> getParameterList() {

        if (this.getParamList() == null) {
            synchronized (this) {
                if (this.getParamList() == null) {
                    // This takes the ordered set and converts to a list
                    // The order is determined by when the param was created.
                    final int size = this.getParamSet().size();
                    this.paramList = Lists.newArrayList(this.getParamSet().toArray(new NamedParameter[size]));
                }
            }
        }
        return this.getParamList();
    }

    public NamedParameter getParameter(final int i) throws HBqlException {
        try {
            return this.getParameterList().get(i - 1);
        }
        catch (Exception e) {
            throw new HBqlException("Invalid index: " + (i - 1));
        }
    }

    public void clearParameters() {
        for (final NamedParameter param : this.getParameterList())
            param.reset();
    }
}
