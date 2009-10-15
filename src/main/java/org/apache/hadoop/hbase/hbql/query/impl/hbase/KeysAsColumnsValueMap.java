package org.apache.hadoop.hbase.hbql.query.impl.hbase;

public class KeysAsColumnsValueMap extends ValueMap<Object> {

    public KeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }
}