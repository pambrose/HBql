package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
import org.apache.hadoop.hbase.hbql.query.schema.HBaseSchema;
import org.apache.hadoop.hbase.hbql.query.util.HUtil;
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
        private final Object actionValue;

        private Action(final Type type, final Object actionValue) {
            this.type = type;
            this.actionValue = actionValue;
        }

        static Action newInsert(final Put put) {
            return new Action(Type.INSERT, put);
        }

        static Action newDelete(final Delete delete) {
            return new Action(Type.DELETE, delete);
        }

        private boolean isInsert() {
            return this.type == Type.INSERT;
        }

        private boolean isDelete() {
            return this.type == Type.DELETE;
        }

        private Put getPutValue() {
            return (Put)this.actionValue;
        }

        private Delete getDeleteValue() {
            return (Delete)this.actionValue;
        }

        public void apply(final HTable table) throws IOException {

            if (this.isInsert())
                table.put(this.getPutValue());

            if (this.isDelete())
                table.delete(this.getDeleteValue());
        }

        public String toString() {
            return this.type.name();
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

    public void insert(final Object newrec) throws HBqlException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        final Put put = createPut(schema, newrec);
        this.getActionList(schema.getTableName()).add(Action.newInsert(put));
    }

    public void insert(final HRecord hRecord) throws HBqlException {
        final HBaseSchema schema = hRecord.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!hRecord.isCurrentValueSet(keyAttrib))
            throw new HBqlException("HRecord key value must be assigned");

        final Put put = createPut(schema, hRecord);
        this.getActionList(schema.getTableName()).add(Action.newInsert(put));
    }

    public void delete(final Object newrec) throws HBqlException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        this.delete(schema, newrec);
    }

    public void delete(final HRecord hRecord) throws HBqlException {
        final HBaseSchema schema = hRecord.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!hRecord.isCurrentValueSet(keyAttrib))
            throw new HBqlException("HRecord key value must be assigned");
        this.delete(schema, hRecord);
    }

    private void delete(HBaseSchema schema, final Object newrec) throws HBqlException {
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        this.getActionList(schema.getTableName()).add(Action.newDelete(new Delete(keyval)));
    }

    private Put createPut(final HBaseSchema schema, final Object newrec) throws HBqlException {

        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);

        final Put put = new Put(keyval);

        for (final String family : schema.getFamilySet()) {
            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {

                if (attrib.isMapKeysAsColumns()) {
                    final Map mapval = (Map)attrib.getCurrentValue(newrec);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] b = HUtil.ser.getScalarAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                HUtil.ser.getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"), b);
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

    private Put createPut(final HBaseSchema schema, final HRecord hRecord) throws HBqlException {

        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(hRecord);

        final Put put = new Put(keyval);

        for (final String family : schema.getFamilySet()) {
            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {

                if (attrib.isMapKeysAsColumns()) {
                    final Map mapval = (Map)attrib.getCurrentValue(hRecord);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] b = HUtil.ser.getScalarAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                HUtil.ser.getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"), b);
                    }
                }
                else {
                    if (hRecord.isCurrentValueSet(attrib)) {
                        final byte[] b = attrib.getValueAsBytes(hRecord);
                        put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
                    }
                }
            }
        }
        return put;
    }
}
