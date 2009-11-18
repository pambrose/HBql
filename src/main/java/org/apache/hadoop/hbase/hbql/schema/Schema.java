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

import org.antlr.runtime.RecognitionException;
import org.apache.expreval.expr.ExpressionTree;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.expreval.util.Sets;
import org.apache.hadoop.hbase.hbql.antlr.HBqlParser;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.client.HConnection;
import org.apache.hadoop.hbase.hbql.client.ParseException;
import org.apache.hadoop.hbase.hbql.parser.ParserUtil;
import org.apache.hadoop.hbase.hbql.statement.SchemaContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Schema implements Serializable {

    private final String schemaName;
    private final String tableName;
    private final Map<String, ColumnAttrib> columnAttribByVariableNameMap = Maps.newHashMap();
    private final Set<ColumnAttrib> columnAttribSet = Sets.newHashSet();

    private ColumnAttrib keyAttrib = null;
    private List<String> evalList = null;
    private int expressionTreeCacheSize = 25;
    private volatile Map<String, ExpressionTree> evalMap = null;

    protected Schema(final String schemaName, final String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public abstract Collection<String> getSchemaFamilyNames(final HConnection connection) throws HBqlException;

    public Set<ColumnAttrib> getColumnAttribSet() {
        return this.columnAttribSet;
    }

    // *** columnAttribByVariableNameMap calls
    private Map<String, ColumnAttrib> getColumnAttribByVariableNameMap() {
        return this.columnAttribByVariableNameMap;
    }

    public boolean containsVariableName(final String varname) {
        return this.getColumnAttribByVariableNameMap().containsKey(varname);
    }

    public ColumnAttrib getAttribByVariableName(final String name) {
        return this.getColumnAttribByVariableNameMap().get(name);
    }

    public void resetDefaultValues() throws HBqlException {
        for (final ColumnAttrib attrib : this.getColumnAttribSet())
            attrib.resetDefaultValue();
    }

    protected void addAttribToVariableNameMap(final ColumnAttrib attrib,
                                              final String... attribNames) throws HBqlException {

        if (!attrib.isFamilyDefaultAttrib())
            this.getColumnAttribSet().add(attrib);

        for (final String attribName : attribNames) {
            if (this.getColumnAttribByVariableNameMap().containsKey(attribName))
                throw new HBqlException(attribName + " already declared");

            this.getColumnAttribByVariableNameMap().put(attribName, attrib);
        }
    }

    private Map<String, ExpressionTree> getEvalMap() {

        if (this.evalMap == null) {
            synchronized (this) {
                if (this.evalMap == null) {
                    this.evalMap = Maps.newHashMap();
                    this.evalList = Lists.newArrayList();
                }
            }
        }
        return this.evalMap;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    private List<String> getEvalList() {
        return this.evalList;
    }

    public String toString() {
        return this.getSchemaName();
    }

    public int getEvalCacheSize() {
        return this.expressionTreeCacheSize;
    }

    public ColumnAttrib getKeyAttrib() {
        return this.keyAttrib;
    }

    protected void setKeyAttrib(final ColumnAttrib keyAttrib) {
        this.keyAttrib = keyAttrib;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setEvalCacheSize(final int size) {

        if (size > 0) {
            this.expressionTreeCacheSize = size;

            // Reset existing cache
            final Map<String, ExpressionTree> map = this.getEvalMap();
            final List<String> list = this.getEvalList();
            map.clear();
            list.clear();
        }
    }

    public ExpressionTree getExpressionTree(final String str,
                                            final SchemaContext schemaContext) throws ParseException, RecognitionException {

        final Map<String, ExpressionTree> map = this.getEvalMap();
        ExpressionTree expressionTree = map.get(str);

        if (expressionTree == null) {
            final HBqlParser parser = ParserUtil.newHBqlParser(str);
            expressionTree = parser.nodescWhereExpr();
            expressionTree.setSchemaContext(schemaContext);
            this.addToExpressionTreeCache(str, expressionTree);
        }
        else {
            expressionTree.reset();
        }
        return expressionTree;
    }

    private synchronized void addToExpressionTreeCache(final String exprStr, final ExpressionTree expressionTree) {

        final Map<String, ExpressionTree> map = this.getEvalMap();

        if (!map.containsKey(exprStr)) {

            final List<String> list = this.getEvalList();

            list.add(exprStr);
            map.put(exprStr, expressionTree);

            if (list.size() > this.getEvalCacheSize()) {
                final String firstOne = list.get(0);
                map.remove(firstOne);
                list.remove(0);
            }
        }
    }
}
