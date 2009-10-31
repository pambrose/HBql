package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;

public class FamilyDefaultKeysAsColumnsValueMap extends ValueMap<UntypedKeysAsColumnsValueMap> {

    public FamilyDefaultKeysAsColumnsValueMap(final RecordImpl record, final String name) throws HBqlException {
        super(record, name, UntypedKeysAsColumnsValueMap.class);
    }

    public String toString() {
        return "Family default keys as columns for: " + this.getName();
    }
}