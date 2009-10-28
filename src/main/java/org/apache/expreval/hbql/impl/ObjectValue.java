package org.apache.expreval.hbql.impl;

import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

public class ObjectValue extends CurrentAndVersionValue<Object> {

    public ObjectValue(final HRecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }
}
