package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class TypedKeysAsColumnsValueMap extends ValueMap<Object> {

    public TypedKeysAsColumnsValueMap(final RecordImpl record, final String name) throws HBqlException {
        super(record, name, null);
    }

    public String toString() {
        return "Typed Keys as Columns Map for: " + this.getName();
    }
}