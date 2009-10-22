package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class ObjectValue extends CurrentAndVersionValue<Object> {

    public ObjectValue(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }
}
