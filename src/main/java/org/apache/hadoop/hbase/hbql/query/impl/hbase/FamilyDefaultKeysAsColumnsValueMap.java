package org.apache.hadoop.hbase.hbql.query.impl.hbase;

import org.apache.hadoop.hbase.hbql.client.HBqlException;

public class FamilyDefaultKeysAsColumnsValueMap extends ValueMap<UntypedKeysAsColumnsValueMap> {

    public FamilyDefaultKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }

    public String toString() {
        return "Family default keys as columns for: " + this.getName();
    }
}