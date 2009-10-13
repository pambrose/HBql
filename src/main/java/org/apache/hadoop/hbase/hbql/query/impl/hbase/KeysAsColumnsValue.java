package org.apache.hadoop.hbase.hbql.query.impl.hbase;

public class KeysAsColumnsValue extends MappedValue<Object> {

    public KeysAsColumnsValue(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }
}