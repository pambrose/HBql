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

        // TODO Need to allow for key to use getter and setter method
        final byte[] keyval = classSchema.getKeyFieldAttrib().getValueAsBytes(declaringObj);

        final BatchUpdate batchUpdate = new BatchUpdate(keyval);

        for (final String family : classSchema.getFieldAttribMapByFamily().keySet()) {

            for (final FieldAttrib attrib : classSchema.getFieldAttribMapByFamily().get(family)) {

                if (attrib.isMapKeysAsColumns()) {
                    final Map map = (Map)attrib.getValue(declaringObj);
                    for (final Object keyobj : map.keySet()) {
                        final String colname = keyobj.toString();
                        final byte[] byteval = getBytes(map.get(keyobj));

                        // Use family:column-key scheme to avoid column namespace collision
                        batchUpdate.put(attrib.getQualifiedName() + "-" + colname, byteval);
                    }

                }
                else {
                    final byte[] instval = attrib.getValueAsBytes(declaringObj);
                    batchUpdate.put(attrib.getQualifiedName(), instval);
                }
            }
        }

        this.getUpdateList(classSchema.getTableName()).add(batchUpdate);

    }

    private static byte[] getBytes(final Object obj) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.flush();
        return baos.toByteArray();
    }

    public void commit() throws IOException {
        for (final String tableName : updateList.keySet()) {
            final HTable table = new HTable(new HBaseConfiguration(), tableName);
            table.commit(this.getUpdateList(tableName));
        }
    }

}
