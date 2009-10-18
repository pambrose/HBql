package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

public class LimitArgs extends SelectArgs {

    public LimitArgs(final GenericValue arg0) {
        super(SelectArgs.Type.LIMIT, arg0);
    }

    public long getValue() throws HBqlException {
        return ((Number)this.evaluateWithoutColumns(0, false, null)).longValue();
    }

    public String asString() {
        return "LIMIT " + this.getGenericValue(0).asString();
    }
}