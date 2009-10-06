package org.apache.hadoop.hbase.hbql.query.stmt.args;

import org.apache.hadoop.hbase.hbql.client.HBqlException;
import org.apache.hadoop.hbase.hbql.query.expr.node.GenericValue;

/**
 * Created by IntelliJ IDEA.
 * User: pambrose
 * Date: Sep 4, 2009
 * Time: 10:26:18 AM
 */
public class LimitArgs extends SelectArgs {

    public LimitArgs(final GenericValue arg0) {
        super(SelectArgs.Type.LIMIT, arg0);
    }

    public long getValue() throws HBqlException {
        return ((Number)this.evaluate(0, false, false, null)).longValue();
    }

    public String asString() {
        return "LIMIT " + this.getGenericValue(0).asString();
    }

}