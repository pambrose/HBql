package org.apache.hadoop.hbase.hbql.query.antlr.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class LimitArgs extends SelectArgs {


    public LimitArgs(final GenericValue value) {
        super(SelectArgs.Type.LIMIT, value);
    }

    public long getValue() throws HBqlException {
        this.validateTypes();
        return ((Number)this.getArg(0).getValue(null)).longValue();
    }
}