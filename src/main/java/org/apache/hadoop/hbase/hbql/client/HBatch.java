package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.schema.HUtil;
import org.apache.hadoop.hbase.hbql.query.util.Lists;
import org.apache.hadoop.hbase.hbql.query.util.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:22:40 PM
 */
public class HBatch {

    public static class Action {
        public enum Type {
            INSERT, DELETE
        }

        private final Type type;
        private final Put putValue;
        private final Delete deleteValue;

        public Action(final Type type, final Put putValue, final Delete deleteValue) {
            this.type = type;
            this.putValue = putValue;
            this.deleteValue = deleteValue;
        }

        static Action newInsert(final Put put) {
            return new Action(Type.INSERT, put, null);
        }

        static Action newDelete(final Delete delete) {
            return new Action(Type.DELETE, null, delete);
        }

        public boolean isInsert() {
            return this.type == Type.INSERT;
        }

        public boolean isDelete() {
            return this.type == Type.DELETE;
        }

        public Put getPutValue() {
            return putValue;
        }

        public Delete getDeleteValue() {
            return deleteValue;
        }
    }

    private final Map<String, List<Action>> actionList = Maps.newHashMap();

    Map<String, List<Action>> getActionList() {
        return this.actionList;
    }

    public synchronized List<Action> getActionList(final String tableName) {
        List<Action> retval = this.getActionList().get(tableName);
        if (retval == null) {
            retval = Lists.newArrayList();
            this.getActionList().put(tableName, retval);
        }
        return retval;
    }

    public void insert(final Object newrec) throws HBqlException, IOException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        this.insert(schema, newrec);
    }

    public void insert(final HRecord newrec) throws HBqlException, IOException {
        final HBaseSchema schema = newrec.getSchema();

        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!newrec.isCurrentValueSet(keyAttrib))
            throw new HBqlException("HRecord key value must be assigned");

        this.insert(schema, newrec);
    }

    public void delete(final Object newrec) throws HBqlException, IOException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        this.delete(schema, newrec);
    }

    public void delete(final HRecord newrec) throws HBqlException, IOException {
        final HBaseSchema schema = newrec.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!newrec.isCurrentValueSet(keyAttrib))
            throw new HBqlException("HRecord key value must be assigned");
        this.delete(schema, newrec);
    }

    private void insert(HBaseSchema schema, final Object newrec) throws IOException, HBqlException {
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        final Put put = createPut(schema, newrec, keyval);
        this.getActionList(schema.getTableName()).add(Action.newInsert(put));
    }

    private void delete(HBaseSchema schema, final Object newrec) throws IOException, HBqlException {
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        this.getActionList(schema.getTableName()).add(Action.newDelete(new Delete(keyval)));
    }

    private Put createPut(final HBaseSchema schema,
                          final Object newrec,
                          final byte[] keyval) throws HBqlException, IOException {
        final Put put = new Put(keyval);
        for (final String family : schema.getFamilySet()) {
            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {
                if (attrib.isMapKeysAsColumns()) {
                    final Map mapval = (Map)attrib.getCurrentValue(newrec);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] byteval = HUtil.ser.getObjectAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                HUtil.ser.getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"),
                                byteval);
                    }
                }
                else {
                    final byte[] instval = attrib.getValueAsBytes(newrec);
                    put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), instval);
                }
            }
        }
        return put;
    }
}
