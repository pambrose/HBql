package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.hbql.impl.HRecordImpl;
import org.apache.expreval.schema.AnnotationSchema;
import org.apache.expreval.schema.ColumnAttrib;
import org.apache.expreval.schema.HBaseSchema;
import org.apache.expreval.util.HUtil;
import org.apache.expreval.util.Lists;
import org.apache.expreval.util.Maps;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;
import java.util.Map;

public class HBatch {

    private final Map<String, List<HBatchAction>> actionList = Maps.newHashMap();

    public Map<String, List<HBatchAction>> getActionList() {
        return this.actionList;
    }

    public synchronized List<HBatchAction> getActionList(final String tableName) {
        List<HBatchAction> retval = this.getActionList().get(tableName);
        if (retval == null) {
            retval = Lists.newArrayList();
            this.getActionList().put(tableName, retval);
        }
        return retval;
    }

    public void insert(final Object newrec) throws HBqlException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        final Put put = createPut(schema, newrec);
        this.getActionList(schema.getTableName()).add(HBatchAction.newInsert(put));
    }

    public void insert(final HRecord rec) throws HBqlException {
        final HRecordImpl hrecord = (HRecordImpl)rec;
        final HBaseSchema schema = hrecord.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!hrecord.isCurrentValueSet(keyAttrib))
            throw new HBqlException("HRecord key value must be assigned");

        final Put put = createPut(schema, hrecord);
        this.getActionList(schema.getTableName()).add(HBatchAction.newInsert(put));
    }

    public void delete(final Object newrec) throws HBqlException {
        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(newrec);
        this.delete(schema, newrec);
    }

    public void delete(final HRecordImpl hrecord) throws HBqlException {
        final HBaseSchema schema = hrecord.getSchema();
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        if (!hrecord.isCurrentValueSet(keyAttrib))
            throw new HBqlException("HRecord key value must be assigned");
        this.delete(schema, hrecord);
    }

    private void delete(HBaseSchema schema, final Object newrec) throws HBqlException {
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(newrec);
        this.getActionList(schema.getTableName()).add(HBatchAction.newDelete(new Delete(keyval)));
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
                        final byte[] b = HUtil.getSerialization().getScalarAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                HUtil.getSerialization().getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"), b);
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

    private Put createPut(final HBaseSchema schema, final HRecordImpl hrecord) throws HBqlException {

        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(hrecord);

        final Put put = new Put(keyval);

        for (final String family : schema.getFamilySet()) {
            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {

                if (attrib.isMapKeysAsColumnsAttrib()) {
                    final Map mapval = (Map)attrib.getCurrentValue(hrecord);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] b = HUtil.getSerialization().getScalarAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyNameAsBytes(),
                                HUtil.getSerialization().getStringAsBytes(attrib.getColumnName() + "[" + colname + "]"), b);
                    }
                }
                else {
                    if (hrecord.isCurrentValueSet(attrib)) {
                        final byte[] b = attrib.getValueAsBytes(hrecord);
                        put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), b);
                    }
                }
            }
        }
        return put;
    }
}
