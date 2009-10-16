package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class UntypedKeysAsColumnsValueMap extends ValueMap<byte[]> {

    public UntypedKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }
}