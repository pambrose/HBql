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

package org.apache.hadoop.hbase.hbql.statement;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.InvalidColumnException;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.statement.args.WithArgs;
import org.apache.hadoop.hbase.hbql.statement.select.SelectElement;
import org.apache.hadoop.hbase.hbql.util.Lists;
import org.apache.hadoop.hbase.hbql.util.Sets;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectStatement extends StatementWithParameters implements HBqlStatement {

    private final List<ColumnAttrib> selectColumnAttribList = Lists.newArrayList();
    private final AtomicInteger expressionCounter = new AtomicInteger(-1);
    private final List<SelectElement> selectElementList;
    private final WithArgs withArgs;

    private boolean validated = false;
    private boolean aggregateQuery = false;

    public SelectStatement(final List<SelectElement> selectElementList,
                           final String mappingName,
                           final WithArgs withArgs) {
        super(null, mappingName);
        this.selectElementList = selectElementList;
        this.withArgs = withArgs != null ? withArgs : new WithArgs();
    }

    public synchronized String getNextExpressionName() {
        return "expr-" + this.expressionCounter.incrementAndGet();
    }

    private boolean isValidated() {
        return this.validated;
    }

    public List<SelectElement> getSelectElementList() {
        return this.selectElementList;
    }

    public List<ColumnAttrib> getSelectAttribList() {
        return this.selectColumnAttribList;
    }

    public WithArgs getWithArgs() {
        return this.withArgs;
    }

    public synchronized void validate(final HConnectionImpl conn) throws HBqlException {

        if (this.isValidated())
            return;

        this.validated = true;

        this.getMappingContext().validateMappingName(conn);

        this.getSelectAttribList().clear();

        for (final SelectElement element : this.getSelectElementList()) {
            element.validate(this.getMappingContext(), conn);
            element.assignAsNamesForExpressions(this);
            this.getSelectAttribList().addAll(element.getAttribsUsedInExpr());
        }

        this.getWithArgs().setMappingContext(this.getMappingContext());
        this.getWithArgs().validate(conn, this.getMappingContext().getTableMapping());

        // Make sure there are no duplicate aliases in list
        this.checkForDuplicateAsNames();

        // Build sorted set of parameters
        this.collectParameters();
    }

    public void validateTypes() throws HBqlException {
        this.getWithArgs().validateArgTypes();
    }

    public void determineIfAggregateQuery() throws HBqlException {

        // This is required before the checkIfAggregateQuery() call.
        for (final SelectElement element : this.getSelectElementList()) {
            try {
                element.validateTypes(true, false);
            }
            catch (InvalidColumnException e) {
                // No op
                e.printStackTrace();
            }
        }

        this.aggregateQuery = this.checkIfAggregateQuery();
    }

    private boolean checkIfAggregateQuery() throws HBqlException {
        final SelectElement firstElement = this.getSelectElementList().get(0);
        final boolean firstIsAggregate = firstElement.isAnAggregateElement();
        for (final SelectElement selectElement : this.getSelectElementList()) {
            if (selectElement.isAnAggregateElement() != firstIsAggregate)
                throw new HBqlException("Cannot mix aggregate and non-aggregate select elements");
        }
        return firstIsAggregate;
    }

    public boolean isAnAggregateQuery() {
        return this.aggregateQuery;
    }

    private void checkForDuplicateAsNames() throws HBqlException {
        final Set<String> asNameSet = Sets.newHashSet();
        for (final SelectElement selectElement : this.getSelectElementList()) {
            if (selectElement.hasAsName()) {
                final String asName = selectElement.getAsName();
                if (asNameSet.contains(asName))
                    throw new HBqlException("Duplicate select name " + asName + " in select list");
                asNameSet.add(asName);
            }
        }
    }

    public boolean hasAsName(final String name) {
        for (final SelectElement selectElement : this.getSelectElementList())
            if (selectElement.hasAsName() && selectElement.getAsName().equals(name))
                return true;
        return false;
    }

    private void collectParameters() {
        for (final SelectElement selectElement : this.getSelectElementList())
            this.getNamedParameters().addParameters(selectElement.getParameterList());

        this.getNamedParameters().addParameters(this.getWithArgs().getParameterList());
    }

    public void resetParameters() {
        for (final SelectElement selectElement : this.getSelectElementList())
            selectElement.reset();

        this.getWithArgs().reset();
    }


    public int setStatementParameter(final String name, final Object val) throws HBqlException {
        int cnt = 0;
        for (final SelectElement selectElement : this.getSelectElementList())
            cnt += selectElement.setParameter(name, val);

        cnt += this.getWithArgs().setParameter(name, val);
        return cnt;
    }

    public String asString() {
        final StringBuilder sbuf = new StringBuilder("SELECT  ");
        boolean firstTime = true;
        for (final SelectElement element : this.getSelectElementList()) {
            if (!firstTime)
                sbuf.append(", ");
            firstTime = false;

            sbuf.append(element.asString());
        }

        sbuf.append(" FROM ");
        sbuf.append(this.getMappingContext().getMappingName());
        sbuf.append(" ");
        sbuf.append(this.getWithArgs().asString());
        return sbuf.toString();
    }

    public static String usage() {
        return "SELECT select_element_list FROM [MAPPING] mapping_name with_clause";
    }
}
