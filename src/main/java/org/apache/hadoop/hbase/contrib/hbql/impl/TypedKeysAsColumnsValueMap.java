package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;

public class TypedKeysAsColumnsValueMap extends ValueMap<Object> {

    public TypedKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name, null);
    }

    public String toString() {
        return "Typed Keys as Columns Map for: " + this.getName();
    }
}