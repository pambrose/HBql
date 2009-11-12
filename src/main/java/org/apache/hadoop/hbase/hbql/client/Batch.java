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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.hbql.impl.BatchAction;
import org.apache.hadoop.hbase.hbql.impl.DeleteAction;
import org.apache.hadoop.hbase.hbql.impl.InsertAction;
import org.apache.hadoop.hbase.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.hbql.schema.AnnotationMapping;
import org.apache.hadoop.hbase.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.schema.HBaseSchema;

import java.util.List;
import java.util.Map;

public class Batch {

    private final Map<String, List<BatchAction>> actionList = Maps.newHashMap();

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
        final AnnotationMapping mapping = AnnotationMapping.getAnnotationMapping(newrec);
        final Put put = this.createPut((HBaseSchema)mapping.getSchema(), newrec);
        this.getActionList(mapping.getSchema().getTableName()).add(new InsertAction(put));
    }

    public void insert(final HRecord rec) throws HBqlException {
        final RecordImpl record = (RecordImpl)rec;

        final HBaseSchema schema = record.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!record.isCurrentValueSet(keyAttrib))
            throw new HBqlException("Record key value must be assigned");

        final Put put = this.createPut(schema, record);
        this.getActionList(schema.getTableName()).add(new InsertAction(put));
    }

    public void delete(final Object newrec) throws HBqlException {
        final AnnotationMapping mapping = AnnotationMapping.getAnnotationMapping(newrec);
        this.delete((HBaseSchema)mapping.getSchema(), newrec);
    }

    public void delete(final RecordImpl record) throws HBqlException {
        final HBaseSchema schema = record.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!record.isCurrentValueSet(keyAttrib))
            throw new HBqlException("Record key value must be assigned");
        this.delete(schema, record);
    }

    private void delete(HBaseSchema schema, final Object newrec) throws HBqlException {
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        this.getActionList(schema.getTableName()).add(new DeleteAction(new Delete(keyval)));
    }

    private Put createPut(final HBaseSchema schema, final Object newrec) throws HBqlException {

        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        final Put put = new Put(keyval);

        for (final String family : schema.getFamilySet()) {
            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {
                final byte[] b = attrib.getValueAsBytes(newrec);
                put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
            }
        }
        return put;
    }

    private Put createPut(final HBaseSchema schema, final RecordImpl record) throws HBqlException {

        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(record);
        final Put put = new Put(keyval);

        for (final String family : schema.getFamilySet()) {
            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {
                if (record.isCurrentValueSet(attrib)) {
                    final byte[] b = attrib.getValueAsBytes(record);
                    put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
                }
            }
        }
        return put;
    }
}
