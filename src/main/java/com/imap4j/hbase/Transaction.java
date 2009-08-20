package com.imap4j.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;

import java.io.IOException;
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

    public void insert(final Persistable obj) throws PersistException {

        final BatchUpdate batchUpdate = new BatchUpdate(obj.getKeyValue());

        final ClassSchema schema = ClassSchema.getClassSchema(obj);

        for (final String family : schema.getFieldAttribs().keySet()) {

            for (final FieldAttrib attrib : schema.getFieldAttribs().get(family)) {

                byte[] val = null;

                Object objval = null;

                try {
                    objval = attrib.getField().get(obj);
                }
                catch (IllegalAccessException e) {
                    throw new PersistException("Error getting value of " + attrib.getField().getName());
                }

                if (objval instanceof String)
                    val = ((String)objval).getBytes();

                batchUpdate.put(family + ":" + attrib.getColumn(), val);
            }
        }

        this.getUpdateList(schema.getTableName()).add(batchUpdate);

    }

    public void commit() throws IOException {
        for (final String tableName : updateList.keySet()) {
            final HTable table = new HTable(new HBaseConfiguration(), tableName);
            table.commit(this.getUpdateList(tableName));
        }
    }

}
