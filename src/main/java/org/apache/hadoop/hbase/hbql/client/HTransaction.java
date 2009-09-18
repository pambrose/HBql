package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.hbql.query.schema.AnnotationSchema;
import org.apache.hadoop.hbase.hbql.query.schema.ColumnAttrib;
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
public class HTransaction {

    private final Map<String, List<Put>> updateList = Maps.newHashMap();

    Map<String, List<Put>> getUpdateList() {
        return this.updateList;
    }

    public synchronized List<Put> getUpdateList(final String tableName) {
        List<Put> retval = this.getUpdateList().get(tableName);
        if (retval == null) {
            retval = Lists.newArrayList();
            this.getUpdateList().put(tableName, retval);
        }
        return retval;
    }

    public void insert(final Object declaringObj) throws HPersistException, IOException {

        final AnnotationSchema schema = AnnotationSchema.getAnnotationSchema(declaringObj);
        final ColumnAttrib keyAttrib = schema.getKeyAttrib();
        final byte[] keyval = keyAttrib.getValueAsBytes(HUtil.ser, declaringObj);
        final Put put = new Put(keyval);

        for (final String family : schema.getFamilyNameList()) {

            for (final ColumnAttrib attrib : schema.getColumnAttribListByFamilyName(family)) {

                if (attrib.isMapKeysAsColumns()) {
                    final Map mapval = (Map)attrib.getCurrentValue(declaringObj);
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
                    final byte[] instval = attrib.getValueAsBytes(HUtil.ser, declaringObj);
                    String key = new String(keyval);
                    String fstr = new String(attrib.getFamilyNameAsBytes());
                    String cstr = new String(attrib.getColumnNameAsBytes());
                    put.add(attrib.getFamilyNameAsBytes(), attrib.getColumnNameAsBytes(), instval);
                }
            }
        }

        this.getUpdateList(schema.getTableName()).add(put);
    }

}
