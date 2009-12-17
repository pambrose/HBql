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
import org.apache.hadoop.hbase.hbql.client.ExecutionResults;
import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.index.SingleColumnIndex;
import org.apache.hadoop.hbase.hbql.mapping.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.mapping.TableMapping;

import java.io.IOException;
import java.util.List;

public class CreateIndexStatement extends MappingStatement implements ConnectionStatement {

    private final String indexName;
    private final List<String> indexColumns;
    private final List<String> includeColumns;

    public CreateIndexStatement(final StatementPredicate predicate,
                                final String indexName,
                                final String mappingName,
                                final List<String> indexColumns,
                                final List<String> includeColumns) {
        super(predicate, mappingName);
        this.indexName = indexName;
        this.indexColumns = indexColumns;
        this.includeColumns = includeColumns;
    }

    private String getIndexName() {
        return this.indexName;
    }

    protected ExecutionResults execute(final HConnectionImpl connection) throws HBqlException {

        final TableMapping mapping = connection.getMapping(this.getMappingName());

        final List<String> indexList = this.getQualifiedNameList(mapping, this.indexColumns);
        final List<String> includeList = this.getQualifiedNameList(mapping, this.includeColumns);
        final IndexSpecification spec = SingleColumnIndex.newIndex(this.getIndexName(), indexList, includeList);

        try {
            connection.getIndexTableAdmin().addIndex(mapping.getTableNameAsBytes(), spec);
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }

        return new ExecutionResults(this.getCreateIndexMsg(indexList, includeList));
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
                                                + " " + column + " in mapping " + this.getMappingName());
                    else
                        retval.add(columnAttrib.getFamilyQualifiedName());
                }
            }
        }

        return retval;
    }

    private String getCreateIndexMsg(final List<String> indexList, final List<String> includeList) {

        final StringBuilder sbuf = new StringBuilder("Index " + this.getIndexName()
                                                     + " created for " + this.getMappingName());

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

    public static String usage() {
        return "CREATE INDEX index_name ON [MAPPING] mapping_name (column) INCLUDE (column_list) [IF boolean_expression]";
    }
}