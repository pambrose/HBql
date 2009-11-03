package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.contrib.hbql.impl.BatchAction;
import org.apache.hadoop.hbase.contrib.hbql.impl.DeleteAction;
import org.apache.hadoop.hbase.contrib.hbql.impl.InsertAction;
import org.apache.hadoop.hbase.contrib.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.contrib.hbql.io.IO;
import org.apache.hadoop.hbase.contrib.hbql.schema.AnnotationSchema;
import org.apache.hadoop.hbase.contrib.hbql.schema.ColumnAttrib;
import org.apache.hadoop.hbase.contrib.hbql.schema.HBaseSchema;

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
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        final Put put = this.createPut(schema, newrec);
        this.getActionList(schema.getTableName()).add(new InsertAction(put));
    }

    public void insert(final Record rec) throws HBqlException {
        final RecordImpl record = (RecordImpl)rec;
        final HBaseSchema schema = record.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!record.isCurrentValueSet(keyAttrib))
            throw new HBqlException("Record key value must be assigned");

        final Put put = this.createPut(schema, record);
        this.getActionList(schema.getTableName()).add(new InsertAction(put));
    }

    public void delete(final Object newrec) throws HBqlException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        this.delete(schema, newrec);
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

                if (attrib.isMapKeysAsColumnsAttrib()) {
                    final Map mapval = (Map)attrib.getCurrentValue(newrec);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] b = IO.getSerialization().getScalarAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                IO.getSerialization().getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"), b);
                    }
                }
                else {
                    final byte[] b = attrib.getValueAsBytes(newrec);
                    put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
                }
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

                if (attrib.isMapKeysAsColumnsAttrib()) {
                    final Map mapval = (Map)attrib.getCurrentValue(record);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] b = IO.getSerialization().getScalarAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                IO.getSerialization().getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"), b);
                    }
                }
                else {
                    if (record.isCurrentValueSet(attrib)) {
                        final byte[] b = attrib.getValueAsBytes(record);
                        put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
                    }
                }
            }
        }
        return put;
    }
}
