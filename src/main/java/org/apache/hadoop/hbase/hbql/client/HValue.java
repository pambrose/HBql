package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;

public abstract class HValue {

    private final String name;

    public HValue(final HRecordImpl hrecord, final String name) throws HBqlException {
        this.name = name;
        if (hrecord != null)
            hrecord.addElement(name, this);
    }

    public String getName() {
        return name;
    }
}
