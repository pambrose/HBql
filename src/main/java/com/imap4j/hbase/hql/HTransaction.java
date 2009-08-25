package com.imap4j.hbase.hql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

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

    public synchronized List<Put> getUpdateList(final String tableName) {
        List<Put> retval = updateList.get(tableName);
        if (retval == null) {
            retval = Lists.newArrayList();
            updateList.put(tableName, retval);
        }
        return retval;
    }

    public void insert(final HPersistable declaringObj) throws HPersistException, IOException {

        final ClassSchema classSchema = ClassSchema.getClassSchema(declaringObj);
        final byte[] keyval = classSchema.getKeyFieldAttrib().getValueAsBytes(declaringObj);
        final Put put = new Put(keyval);

        for (final String family : classSchema.getFieldAttribMapByFamily().keySet()) {

            for (final FieldAttrib attrib : classSchema.getFieldAttribMapByFamily().get(family)) {

                if (attrib.isMapKeysAsColumns()) {
                    final Map mapval = (Map)attrib.getValue(declaringObj);
                    for (final Object keyobj : mapval.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] byteval = HUtil.getObjectAsBytes(mapval.get(keyobj));

                        // Use family:column[key] scheme to avoid column namespace collision
                        put.add(attrib.getFamilyName().getBytes(),
                                (attrib.getColumnName() + "[" + colname + "]").getBytes(),
                                byteval);
                    }
                }
                else {
                    final byte[] instval = attrib.getValueAsBytes(declaringObj);
                    put.add(attrib.getFamilyName().getBytes(),
                            attrib.getColumnName().getBytes(),
                            instval);
                }
            }
        }

        this.getUpdateList(classSchema.getTableName()).add(put);
    }


    public void commit() throws IOException {
        for (final String tableName : updateList.keySet()) {
            final HTable table = new HTable(new HBaseConfiguration(), tableName);
            table.put(this.getUpdateList(tableName));
            table.flushCommits();
        }
    }

}
