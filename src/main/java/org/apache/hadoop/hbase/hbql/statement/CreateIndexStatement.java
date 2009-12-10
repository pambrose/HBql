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

import org.apache.expreval.util.Lists;
import org.apache.hadoop.hbase.client.tableindexed.IndexSpecification;
import org.apache.hadoop.hbase.client.tableindexed.IndexedTableAdmin;
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.index.TypedColumnIndex;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;

import java.io.IOException;
import java.util.List;

public class CreateIndexStatement extends BasicStatement implements ConnectionStatement {

    private final String indexName;
    private final String mappingName;
    private final List<String> indexColumns;
    private final List<String> includeColumns;

    public CreateIndexStatement(final StatementPredicate predicate,
                                final String indexName,
                                final String mappingName,
                                final List<String> indexColumns,
                                final List<String> includeColumns) {
        super(predicate);
        this.indexName = indexName;
        this.mappingName = mappingName;
        this.indexColumns = indexColumns;
        this.includeColumns = includeColumns;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final TableMapping mapping = connection.getMapping(this.mappingName);

        final List<String> indexList = this.getQualifiedNameList(mapping, indexColumns);
        final List<String> includeList = this.getQualifiedNameList(mapping, includeColumns);

        try {
            final IndexSpecification spec = TypedColumnIndex.newTypedColumnIndex(this.indexName,
                                                                                 indexList,
                                                                                 includeList);
            final IndexedTableAdmin ita = connection.getIndexTableAdmin();
            ita.addIndex(this.mappingName.getBytes(), spec);

            return new ExecutionResults(this.getCreateMsg(indexList, includeList));
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }

    public static String usage() {
        return "CREATE INDEX index_name ON [MAPPING] mapping_name (column_list) INCLUDE (column_list) [IF boolean_expression]";
    }

    private List<String> getQualifiedNameList(final TableMapping mapping,
                                              final List<String> columnList) throws HBqlException {

        final List<String> retval;

        if (columnList == null) {
            retval = null;
        }
        else {

            retval = Lists.newArrayList();

            for (final String column : columnList) {
                if (column.endsWith(":*")) {
                    final String familyName = column.substring(0, column.length() - 2);
                    retval.add(familyName);
                }
                else {
                    final ColumnAttrib columnAttrib = mapping.getAttribByVariableName(column);
                    if (columnAttrib == null)
                        throw new HBqlException("Unknown " +
                                                ((!column.contains(":")) ? "alias" : "column")
                                                + " " + column + " in mapping " + mappingName);
                    else
                        retval.add(columnAttrib.getFamilyQualifiedName());
                }
            }
        }

        return retval;
    }

    private String getCreateMsg(final List<String> indexList, final List<String> includeList) {

        final StringBuilder sbuf = new StringBuilder("Index " + this.indexName
                                                     + " created for " + this.mappingName);

        sbuf.append(" (");

        boolean first = true;
        for (final String val : indexList) {
            if (!first)
                sbuf.append(", ");
            else
                first = false;

            sbuf.append(val);
        }

        sbuf.append(")");

        if (includeList != null) {
            sbuf.append(" INCLUDE (");
            first = true;
            for (final String val : includeList) {
                if (!first)
                    sbuf.append(", ");
                else
                    first = false;

                sbuf.append(val);
            }
            sbuf.append(")");
        }

        return sbuf.toString();
    }
}