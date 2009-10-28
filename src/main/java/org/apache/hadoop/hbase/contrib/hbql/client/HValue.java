package org.apache.hadoop.hbase.contrib.hbql.client;

import org.apache.expreval.hbql.impl.HRecordImpl;

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
