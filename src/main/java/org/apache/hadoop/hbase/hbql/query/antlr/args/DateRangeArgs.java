package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 9:53:48 AM
 */
public class DateRangeArgs extends SelectArgs {


    public DateRangeArgs(final GenericValue lower, final GenericValue upper) {
        super(SelectArgs.Type.DATERANGE, lower, upper);
    }


    public long getLower() throws HBqlException {
        this.validateType(0);
        return (Long)this.getArg(0).getValue(null);
    }

    public long getUpper() throws HBqlException {
        this.validateType(1);
        return (Long)this.getArg(1).getValue(null);
    }
}