package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.client.HBqlException;
import org.apache.hadoop.hbase.contrib.hbql.impl.RecordImpl;
import org.apache.hadoop.hbase.contrib.hbql.impl.ValueMap;

public class FamilyDefaultValueMap extends ValueMap<byte[]> {

    public FamilyDefaultValueMap(final RecordImpl record, final String name) throws HBqlException {
        super(record, name, null);
    }

    public String toString() {
        return "Family default for: " + this.getName();
    }
}