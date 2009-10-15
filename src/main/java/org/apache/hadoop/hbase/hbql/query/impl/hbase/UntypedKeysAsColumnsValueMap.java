package org.apache.hadoop.hbase.hbql.query.impl.hbase;

public class UntypedKeysAsColumnsValueMap extends ValueMap<byte[]> {

    public UntypedKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }
}