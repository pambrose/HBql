package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.hbql.impl.HRecordImpl;
import org.apache.expreval.hbql.impl.ValueMap;

public class FamilyDefaultValueMap extends ValueMap<byte[]> {

    public FamilyDefaultValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name, null);
    }

    public String toString() {
        return "Family default for: " + this.getName();
    }
}