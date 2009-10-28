package org.apache.expreval.statement.args;

import org.apache.expreval.expr.node.GenericValue;
import org.apache.hadoop.hbase.contrib.hbql.client.HBqlException;

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