package com.imap4j.hbase.hbql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Aug 19, 2009
 * Time: 4:22:40 PM
 */
public class Transaction {

    private final Map<String, List<BatchUpdate>> updateList = Maps.newHashMap();

    public Transaction() {
    }

    public synchronized List<BatchUpdate> getUpdateList(final String tableName) {
        List<BatchUpdate> retval = updateList.get(tableName);
        if (retval == null) {
            retval = Lists.newArrayList();
            updateList.put(tableName, retval);
        }
        return retval;
    }

    public void insert(final Persistable declaringObj) throws PersistException, IOException {

        final ClassSchema classSchema = ClassSchema.getClassSchema(declaringObj);

        final BatchUpdate batchUpdate = new BatchUpdate();

        for (final String family : classSchema.getFieldAttribMapByFamily().keySet()) {

            for (final FieldAttrib attrib : classSchema.getFieldAttribMapByFamily().get(family)) {

                final Object instanceVarObj;
                try {
                    instanceVarObj = attrib.getField().get(declaringObj);

                    if (instanceVarObj == null)
                        continue;
                }
                catch (IllegalAccessException e) {
                    throw new PersistException("Error getting value of " + attrib.getField().getName());
                }

                if (attrib.isMapKeysAsColumns()) {
                    final Map map = (Map)instanceVarObj;
                    for (final Object keyobj : map.keySet()) {
                        final Object val = map.get(keyobj);
                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        final ObjectOutputStream oos = new ObjectOutputStream(baos);
                        oos.writeObject(instanceVarObj);
                        oos.flush();
                        final byte[] byteval = baos.toByteArray();
                        final String colname = val.toString();
                        batchUpdate.put(family + ":" + colname, byteval);
                    }
                }
                else {
                    final byte[] val;
                    if (attrib.isLookupAttrib())
                        val = attrib.invokeLookupMethod(declaringObj);
                    else
                        val = attrib.asBytes(instanceVarObj);

                    batchUpdate.put(family + ":" + attrib.getColumn(), val);
                }
            }
        }

        this.getUpdateList(classSchema.getTableName()).add(batchUpdate);

    }

    public void commit() throws IOException {
        for (final String tableName : updateList.keySet()) {
            final HTable table = new HTable(new HBaseConfiguration(), tableName);
            table.commit(this.getUpdateList(tableName));
        }
    }

}
