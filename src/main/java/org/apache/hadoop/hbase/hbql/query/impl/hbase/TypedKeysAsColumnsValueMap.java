package org.apache.hadoop.hbase.hbql.query.impl.hbase;

public class TypedKeysAsColumnsValueMap extends ValueMap<Object> {

    public TypedKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }
}