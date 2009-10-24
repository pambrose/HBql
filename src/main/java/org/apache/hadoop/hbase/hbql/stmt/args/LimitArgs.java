package org.apache.hadoop.hbase.hbql.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.stmt.expr.node.GenericValue;

public class LimitArgs extends SelectArgs {

    public LimitArgs(final GenericValue arg0) {
        super(SelectArgs.Type.LIMIT, arg0);
    }

    public long getValue() throws HBqlException {
        return ((Number)this.evaluateConstant(0, false, null)).longValue();
    }

    public String asString() {
        return "LIMIT " + this.getGenericValue(0).asString();
    }
}