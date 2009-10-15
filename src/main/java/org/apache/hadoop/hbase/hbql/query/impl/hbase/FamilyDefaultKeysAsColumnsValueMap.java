package org.apache.hadoop.hbase.hbql.query.impl.hbase;

public class FamilyDefaultKeysAsColumnsValueMap extends ValueMap<UntypedKeysAsColumnsValueMap> {

    public FamilyDefaultKeysAsColumnsValueMap(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }
}