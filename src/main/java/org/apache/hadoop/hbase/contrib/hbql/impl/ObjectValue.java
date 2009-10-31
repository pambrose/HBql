package org.apache.hadoop.hbase.contrib.hbql.impl;

import org.apache.expreval.client.HBqlException;

public class ObjectValue extends CurrentAndVersionValue<Object> {

    public ObjectValue(final RecordImpl hrecord, final String name) throws HBqlException {
        super(hrecord, name);
    }
}
