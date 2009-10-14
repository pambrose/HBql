package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;
import org.apache.hadoop.hbase.hbql.query.impl.hbase.MappedValue;

public class FamilyDefaultValue extends MappedValue<byte[]> {

    public FamilyDefaultValue(final HRecordImpl hrecord, final String name) {
        super(hrecord, name);
    }

    public String toString() {
        return "Family default for family: " + this.getName();
    }
}