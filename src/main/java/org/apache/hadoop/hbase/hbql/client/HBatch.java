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

package org.apache.hadoop.hbase.hbql.client;

import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.hbql.impl.BatchAction;
import org.apache.hadoop.hbase.hbql.impl.DeleteAction;
import org.apache.hadoop.hbase.hbql.impl.HConnectionImpl;
import org.apache.hadoop.hbase.hbql.impl.HRecordImpl;
import org.apache.hadoop.hbase.hbql.impl.InsertAction;
import org.apache.hadoop.hbase.hbql.schema.AnnotationResultMapping;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.HBaseMapping;
import org.apache.hadoop.hbase.hbql.schema.ResultMapping;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HBatch {

    private final HConnection connection;

    private final Map<String, List<BatchAction>> actionList = Maps.newHashMap();

    public HBatch(final HConnection connection) {
        this.connection = connection;
    }

    public HConnection getConnection() {
        return connection;
    }

    private HConnectionImpl getConnectionImpl() {
        return (HConnectionImpl)connection;
    }

    public Map<String, List<BatchAction>> getActionList() {
        return this.actionList;
    }

    public synchronized List<BatchAction> getActionList(final String tableName) {
        List<BatchAction> retval = this.getActionList().get(tableName);
        if (retval == null) {
            retval = Lists.newArrayList();
            this.getActionList().put(tableName, retval);
        }
        return retval;
    }

    public void insert(final Object newrec) throws HBqlException {
        final AnnotationResultMapping mapping = this.getConnectionImpl().getAnnotationMapping(newrec);
        final Put put = this.createPut(mapping, newrec);
        this.getActionList(mapping.getSchema().getTableName()).add(new InsertAction(put));
    }

    public void insert(final HRecord rec) throws HBqlException {
        final HRecordImpl record = (HRecordImpl)rec;

        final HBaseMapping mapping = record.getHBaseSchema();
        final ColumnAttrib keyAttrib = mapping.getKeyAttrib();
        if (!record.isCurrentValueSet(keyAttrib))
            throw new HBqlException("Record key value must be assigned");

        final Put put = this.createPut(record.getMapping(), record);
        this.getActionList(mapping.getTableName()).add(new InsertAction(put));
    }

    public void delete(final Object newrec) throws HBqlException {
        final AnnotationResultMapping mapping = this.getConnectionImpl().getAnnotationMapping(newrec);
        this.delete(mapping.getHBaseSchema(), newrec);
    }

    public void delete(final HRecordImpl record) throws HBqlException {
        final HBaseMapping mapping = record.getHBaseSchema();
        final ColumnAttrib keyAttrib = mapping.getKeyAttrib();
        if (!record.isCurrentValueSet(keyAttrib))
            throw new HBqlException("Record key value must be assigned");
        this.delete(mapping, record);
    }

    private void delete(HBaseMapping mapping, final Object newrec) throws HBqlException {
        final ColumnAttrib keyAttrib = mapping.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        this.getActionList(mapping.getTableName()).add(new DeleteAction(new Delete(keyval)));
    }

    private Put createPut(final ResultMapping resultMapping, final Object newrec) throws HBqlException {

        final HBaseMapping mapping = resultMapping.getHBaseSchema();

        final ColumnAttrib keyAttrib = mapping.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        final Put put = new Put(keyval);

        for (final String family : mapping.getFamilySet()) {
            for (final ColumnAttrib colattrib : mapping.getColumnAttribListByFamilyName(family)) {

                // One extra lookup for annotations
                final ColumnAttrib attrib = mapping.getAttribByVariableName(colattrib.getFamilyQualifiedName());
                final byte[] b = attrib.getValueAsBytes(newrec);
                put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
            }
        }
        return put;
    }

    private Put createPut(final ResultMapping resultMapping, final HRecordImpl record) throws HBqlException {

        final HBaseMapping mapping = resultMapping.getHBaseSchema();

        final ColumnAttrib keyAttrib = mapping.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(record);
        final Put put = new Put(keyval);

        for (final String family : mapping.getFamilySet()) {
            for (final ColumnAttrib attrib : mapping.getColumnAttribListByFamilyName(family)) {
                if (record.isCurrentValueSet(attrib)) {
                    final byte[] b = attrib.getValueAsBytes(record);
                    put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
                }
            }
        }
        return put;
    }

    public void apply() throws HBqlException {
        try {
            for (final String tableName : this.getActionList().keySet()) {
                final HTable table = this.getConnection().newHTable(tableName);
                for (final BatchAction batchAction : this.getActionList(tableName))
                    batchAction.apply(table);
                table.flushCommits();
            }
        }
        catch (IOException e) {
            throw new HBqlException(e);
        }
    }
}
