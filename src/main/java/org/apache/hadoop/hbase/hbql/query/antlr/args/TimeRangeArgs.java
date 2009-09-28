package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class TimeRangeArgs extends SelectArgs {

    public TimeRangeArgs(final GenericValue arg0, final GenericValue arg1) {
        super(SelectArgs.Type.TIMERANGE, arg0, arg1);
    }

    public long getLower() throws HBqlException {
        this.validateType(0);
        return (Long)this.getArg(0).getValue(null);
    }

    public long getUpper() throws HBqlException {
        this.validateType(1);
        return (Long)this.getArg(1).getValue(null);
    }

    public String asString() {
        return "TIME RANGE " + this.getArg(0).asString() + " TO " + this.getArg(1);
    }
}