package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;

public class FamilyDefaultKeysAsColumnsValueMap extends ValueMap<UntypedKeysAsColumnsValueMap> {

    public FamilyDefaultKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name, UntypedKeysAsColumnsValueMap.class);
    }

    public String toString() {
        return "Family default keys as columns for: " + this.getName();
    }
}