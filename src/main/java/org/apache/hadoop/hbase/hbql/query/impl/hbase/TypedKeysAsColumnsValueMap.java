package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class TypedKeysAsColumnsValueMap extends ValueMap<Object> {

    public TypedKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name, Object.class);
    }
}