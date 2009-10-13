package org.apache.hadoop.hbase.hbql.client;

import org.apache.hadoop.hbase.hbql.query.impl.hbase.HRecordImpl;

public abstract class HValue {

    public HValue(final HRecordImpl hrecord, final String name) {
        if (hrecord != null)
            hrecord.getValuesMap().put(name, this);
    }
}
