package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;

public class UntypedKeysAsColumnsValueMap extends ValueMap<byte[]> {

    public UntypedKeysAsColumnsValueMap() throws HBqlException {
        super(null, null, null);
    }

    public String toString() {
        return "Untyped Keys as Columns Map for: " + this.getName();
    }
}